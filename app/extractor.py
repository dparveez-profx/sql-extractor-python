"""
Core SQL extraction logic.

Parses a SQL query using mo-sql-parsing and extracts a mapping of
table names to the columns referenced against them, plus a list of
ambiguous columns that cannot be attributed to a single table.

When an optional *schema* (table → columns map) is provided, ambiguous
columns are resolved by looking up which schema tables actually own
them.  If a column belongs to more than one in-scope table according
to the schema an ``AmbiguousColumnError`` is raised.
"""

from __future__ import annotations

from mo_sql_parsing import parse as sql_parse

# Every JOIN variant that mo-sql-parsing may emit as a key.
_JOIN_KEYS = {
    "join",
    "inner join",
    "left join",
    "left outer join",
    "right join",
    "right outer join",
    "full join",
    "full outer join",
    "cross join",
}


class AmbiguousColumnError(Exception):
    """Raised when a column cannot be resolved to a single table even
    after consulting the provided schema."""

    def __init__(self, column: str, tables: list[str]) -> None:
        self.column = column
        self.tables = sorted(tables)
        super().__init__(
            f"Column '{column}' is ambiguous — present in tables: "
            f"{', '.join(self.tables)}"
        )


def extract(
    sql: str,
    schema: dict[str, list[str]] | None = None,
) -> dict:
    """
    Parse *sql* and return a dict with two keys:

    * ``tables``   – ``{table_name: sorted list of columns}``
    * ``ambiguous`` – sorted list of columns that could not be resolved
                      to exactly one table.

    Columns that are qualified (``t.col``) are attributed to the real
    table name even when an alias is used.  Unqualified columns are
    attributed to the single table in scope when only one table exists;
    otherwise they land in *ambiguous*.

    If *schema* is supplied (a mapping of ``{table_name: [col, …]}``),
    every ambiguous column is looked up in the schema.  When exactly one
    in-scope table contains that column it is resolved automatically.
    When **multiple** in-scope tables contain it, an
    ``AmbiguousColumnError`` is raised.
    """
    ast = sql_parse(sql)
    ctx = _ExtractionContext(schema=schema)
    ctx.process_query(ast)
    return ctx.result()


# ── internal helpers ────────────────────────────────────────────────


class _ExtractionContext:
    """Mutable accumulator that walks an mo-sql-parsing AST."""

    def __init__(
        self,
        schema: dict[str, list[str]] | None = None,
    ) -> None:
        # {real_table_name: set(column_names)}
        self._tables: dict[str, set[str]] = {}
        # columns we cannot resolve
        self._ambiguous: set[str] = set()
        # optional real schema for disambiguation
        # normalise to sets for fast lookup
        self._schema: dict[str, set[str]] | None = (
            {t: set(cols) for t, cols in schema.items()} if schema else None
        )

    # ── public entry point ──────────────────────────────────────────

    def process_query(self, ast: dict | list) -> None:
        """Dispatch on top-level set operators or a single SELECT."""
        if isinstance(ast, list):
            for item in ast:
                self.process_query(item)
            return

        # UNION / UNION ALL / INTERSECT / EXCEPT
        for set_op in ("union", "union_all", "intersect", "except"):
            if set_op in ast:
                for branch in ast[set_op]:
                    self.process_query(branch)
                return

        # Regular SELECT
        self._process_select(ast)

    # ── SELECT processing ───────────────────────────────────────────

    def _process_select(self, ast: dict) -> None:
        # 1. Collect tables & aliases from FROM / JOINs
        alias_map: dict[str, str] = {}          # alias -> real table
        tables_in_scope: list[str] = []          # real table names

        from_clause = ast.get("from")
        if from_clause is not None:
            self._collect_tables(from_clause, alias_map, tables_in_scope)

        # 2. Walk every clause that may reference columns
        col_refs: list[str] = []

        # ON clauses from JOINs
        if from_clause is not None:
            self._collect_on_columns(from_clause, col_refs)

        for key in ("select", "select_distinct"):
            if key in ast:
                self._collect_columns(ast[key], col_refs)
                self._collect_wildcards(ast[key], alias_map, tables_in_scope)

        for key in ("where", "having"):
            if key in ast:
                self._collect_columns(ast[key], col_refs)

        if "groupby" in ast:
            self._collect_columns(ast["groupby"], col_refs)

        if "orderby" in ast:
            self._collect_columns(ast["orderby"], col_refs)

        # 3. Attribute each column reference
        for ref in col_refs:
            self._attribute(ref, alias_map, tables_in_scope)

    # ── FROM / JOIN table collection ────────────────────────────────

    def _collect_tables(
        self,
        node,
        alias_map: dict[str, str],
        tables_in_scope: list[str],
    ) -> None:
        """Recursively collect real table names and alias mappings."""
        if isinstance(node, str):
            # Simple table name, no alias
            self._ensure_table(node)
            tables_in_scope.append(node)
            return

        if isinstance(node, list):
            for item in node:
                self._collect_tables(item, alias_map, tables_in_scope)
            return

        if isinstance(node, dict):
            # Check for JOIN variants
            for jk in _JOIN_KEYS:
                if jk in node:
                    self._collect_tables(node[jk], alias_map, tables_in_scope)
                    # ON clause may reference columns
                    # (we don't need to collect them into col_refs here –
                    #  they will be qualified or handled by _attribute)
                    if "on" in node:
                        # We handle ON columns as part of the query scope
                        pass
                    return

            # Subquery in FROM  – {"value": {subquery}, "name": "alias"}
            if "value" in node:
                value = node["value"]
                alias = node.get("name")
                if isinstance(value, dict) and ("select" in value or "select_distinct" in value):
                    # Recurse into subquery (separate scope)
                    self.process_query(value)
                    # The alias acts as a "table" in the outer scope but
                    # maps to no real table – we still register it so
                    # qualified refs like sub.col can be captured.
                    if alias:
                        # Subquery alias – we don't map it to a real table
                        # but we note it so qualified refs are not lost.
                        self._ensure_table(alias)
                        tables_in_scope.append(alias)
                    return
                elif isinstance(value, str):
                    # Table with alias  – {"value": "t1", "name": "x"}
                    real = value
                    self._ensure_table(real)
                    tables_in_scope.append(real)
                    if alias:
                        alias_map[alias] = real
                    return

    # ── ON-clause column collection ───────────────────────────────

    def _collect_on_columns(self, node, col_refs: list[str]) -> None:
        """Walk the FROM clause and pull column refs out of ON conditions."""
        if isinstance(node, str):
            return
        if isinstance(node, list):
            for item in node:
                self._collect_on_columns(item, col_refs)
            return
        if isinstance(node, dict):
            if "on" in node:
                self._collect_columns(node["on"], col_refs)
            # Recurse into JOIN values in case of nested joins
            for jk in _JOIN_KEYS:
                if jk in node:
                    self._collect_on_columns(node[jk], col_refs)

    # ── Wildcard (*) collection ───────────────────────────────────

    def _collect_wildcards(
        self,
        node,
        alias_map: dict[str, str],
        tables_in_scope: list[str],
    ) -> None:
        """Find ``{"all_columns": ...}`` nodes and attribute ``*``."""
        if isinstance(node, list):
            for item in node:
                self._collect_wildcards(item, alias_map, tables_in_scope)
            return

        if not isinstance(node, dict):
            return

        if "all_columns" not in node:
            return

        qualifier = node["all_columns"]
        if isinstance(qualifier, str):
            # Qualified: SELECT t1.* → attribute * to that table
            real_table = alias_map.get(qualifier, qualifier)
            self._ensure_table(real_table)
            self._tables[real_table].add("*")
        else:
            # Unqualified: SELECT * → attribute * to every table in scope
            for table in tables_in_scope:
                self._ensure_table(table)
                self._tables[table].add("*")

    # ── Column reference collection ─────────────────────────────────

    def _collect_columns(self, node, col_refs: list[str]) -> None:
        """Walk an AST node and collect every column-reference string."""
        if isinstance(node, str):
            # Skip the wildcard "*" – it's not a real column name
            if node != "*":
                col_refs.append(node)
            return

        if isinstance(node, (int, float, bool)) or node is None:
            return

        if isinstance(node, list):
            for item in node:
                self._collect_columns(item, col_refs)
            return

        if isinstance(node, dict):
            # {"all_columns": {}} → SELECT *  – nothing to collect
            if "all_columns" in node:
                return

            # {"literal": "..."} → SQL string / date literal, not a column
            if "literal" in node:
                return

            # {"value": ..., "name": ...} → SELECT expr AS alias
            if "value" in node:
                self._collect_columns(node["value"], col_refs)
                # "name" is an alias, not a column reference – skip it
                return

            # Subquery appearing in expressions (WHERE IN (SELECT ...))
            if "select" in node or "select_distinct" in node:
                self.process_query(node)
                return

            # Set operations nested inside expressions
            for set_op in ("union", "union_all", "intersect", "except"):
                if set_op in node:
                    self.process_query(node)
                    return

            # {"exists": {subquery}}
            if "exists" in node:
                val = node["exists"]
                if isinstance(val, dict) and ("select" in val or "select_distinct" in val):
                    self.process_query(val)
                    return

            # Operators / functions: {"gt": [...], "count": "col", ...}
            for key, val in node.items():
                self._collect_columns(val, col_refs)

    # ── Attribution ─────────────────────────────────────────────────

    def _attribute(
        self,
        ref: str,
        alias_map: dict[str, str],
        tables_in_scope: list[str],
    ) -> None:
        """Assign a column reference to a table or mark it ambiguous."""
        parts = ref.split(".")
        if len(parts) == 2:
            qualifier, column = parts
            # Resolve alias → real table name
            real_table = alias_map.get(qualifier, qualifier)
            self._ensure_table(real_table)
            self._tables[real_table].add(column)
        elif len(parts) == 1:
            column = parts[0]
            if len(tables_in_scope) == 1:
                table = tables_in_scope[0]
                self._ensure_table(table)
                self._tables[table].add(column)
            elif self._schema is not None:
                self._disambiguate(column, tables_in_scope)
            else:
                self._ambiguous.add(column)
        # len > 2: schema.table.col – treat first two as qualifier
        elif len(parts) >= 3:
            *qualifier_parts, column = parts
            qualifier = ".".join(qualifier_parts)
            real_table = alias_map.get(qualifier, qualifier)
            self._ensure_table(real_table)
            self._tables[real_table].add(column)

    # ── Schema-based disambiguation ────────────────────────────────

    def _disambiguate(
        self,
        column: str,
        tables_in_scope: list[str],
    ) -> None:
        """Resolve *column* using ``self._schema``.

        Look at every in-scope table that appears in the schema and
        check whether *column* is listed there.

        * **1 match** → attribute the column to that table.
        * **>1 match** → raise ``AmbiguousColumnError``.
        * **0 matches** → column is not in the schema at all; leave it
          in the *ambiguous* list (the user's schema may be incomplete).
        """
        assert self._schema is not None  # caller guarantees this

        owners: list[str] = [
            table
            for table in tables_in_scope
            if table in self._schema and column in self._schema[table]
        ]

        if len(owners) == 1:
            self._ensure_table(owners[0])
            self._tables[owners[0]].add(column)
        elif len(owners) > 1:
            raise AmbiguousColumnError(column, owners)
        else:
            # Column not found in any schema table – keep it ambiguous
            self._ambiguous.add(column)

    # ── Helpers ─────────────────────────────────────────────────────

    def _ensure_table(self, name: str) -> None:
        if name not in self._tables:
            self._tables[name] = set()

    def result(self) -> dict:
        return {
            "tables": {t: sorted(cols) for t, cols in sorted(self._tables.items())},
            "ambiguous": sorted(self._ambiguous),
        }
