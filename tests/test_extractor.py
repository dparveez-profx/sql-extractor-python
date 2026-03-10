"""
Unit tests for the SQL extractor core logic.

Covers: SELECT, FROM, WHERE, GROUP BY, HAVING, ORDER BY, LIKE,
        JOINs (INNER, LEFT, RIGHT, FULL OUTER, CROSS),
        UNION, INTERSECT, EXCEPT, nested subqueries, aliases,
        aggregate functions, DISTINCT, EXISTS, IN-subqueries,
        and ambiguous-column detection.
"""

from __future__ import annotations

import pytest

from app.extractor import extract


# ── Helpers ─────────────────────────────────────────────────────────


def tables(result: dict) -> dict[str, list[str]]:
    return result["tables"]


def ambiguous(result: dict) -> list[str]:
    return result["ambiguous"]


# ====================================================================
# 1. Basic SELECT / FROM
# ====================================================================


class TestBasicSelect:
    def test_single_column_single_table(self):
        r = extract("SELECT a FROM t1")
        assert tables(r) == {"t1": ["a"]}
        assert ambiguous(r) == []

    def test_multiple_columns_single_table(self):
        r = extract("SELECT a, b, c FROM t1")
        assert tables(r) == {"t1": ["a", "b", "c"]}

    def test_select_star(self):
        """SELECT * should register the wildcard against the table."""
        r = extract("SELECT * FROM t1")
        assert tables(r) == {"t1": ["*"]}
        assert ambiguous(r) == []

    def test_qualified_columns(self):
        r = extract("SELECT t1.a, t1.b FROM t1")
        assert tables(r) == {"t1": ["a", "b"]}

    def test_column_alias_not_captured(self):
        """Aliases in SELECT list should not appear as column names."""
        r = extract("SELECT a AS alias_a FROM t1")
        assert tables(r) == {"t1": ["a"]}


# ====================================================================
# 2. WHERE clause
# ====================================================================


class TestWhere:
    def test_simple_where(self):
        r = extract("SELECT a FROM t1 WHERE b > 5")
        assert tables(r) == {"t1": ["a", "b"]}

    def test_where_and(self):
        r = extract("SELECT a FROM t1 WHERE b > 5 AND c = 'x'")
        assert tables(r) == {"t1": ["a", "b", "c"]}

    def test_where_or(self):
        r = extract("SELECT a FROM t1 WHERE b > 5 OR c < 10")
        assert tables(r) == {"t1": ["a", "b", "c"]}

    def test_where_between(self):
        r = extract("SELECT a FROM t1 WHERE b BETWEEN 1 AND 10")
        assert tables(r) == {"t1": ["a", "b"]}

    def test_where_in_list(self):
        r = extract("SELECT a FROM t1 WHERE b IN (1, 2, 3)")
        assert tables(r) == {"t1": ["a", "b"]}

    def test_where_is_null(self):
        r = extract("SELECT a FROM t1 WHERE b IS NULL")
        assert tables(r) == {"t1": ["a", "b"]}

    def test_where_is_not_null(self):
        r = extract("SELECT a FROM t1 WHERE b IS NOT NULL")
        assert tables(r) == {"t1": ["a", "b"]}


# ====================================================================
# 3. LIKE
# ====================================================================


class TestLike:
    def test_like_in_where(self):
        r = extract("SELECT a FROM t1 WHERE b LIKE '%foo%'")
        assert tables(r) == {"t1": ["a", "b"]}

    def test_not_like(self):
        r = extract("SELECT a FROM t1 WHERE b NOT LIKE 'bar%'")
        assert tables(r) == {"t1": ["a", "b"]}

    def test_like_combined_with_and(self):
        r = extract("SELECT a FROM t1 WHERE b LIKE '%x%' AND c = 1")
        assert tables(r) == {"t1": ["a", "b", "c"]}


# ====================================================================
# 4. GROUP BY / HAVING / ORDER BY
# ====================================================================


class TestGroupByOrderBy:
    def test_group_by(self):
        r = extract("SELECT a, COUNT(b) FROM t1 GROUP BY a")
        assert tables(r) == {"t1": ["a", "b"]}

    def test_group_by_multiple(self):
        r = extract("SELECT a, b, COUNT(c) FROM t1 GROUP BY a, b")
        assert tables(r) == {"t1": ["a", "b", "c"]}

    def test_having(self):
        r = extract(
            "SELECT a, SUM(b) FROM t1 GROUP BY a HAVING SUM(b) > 10"
        )
        assert tables(r) == {"t1": ["a", "b"]}

    def test_having_with_avg(self):
        r = extract(
            "SELECT dept, AVG(salary) FROM employees GROUP BY dept HAVING AVG(salary) > 50000"
        )
        assert tables(r) == {"employees": ["dept", "salary"]}

    def test_order_by(self):
        r = extract("SELECT a, b FROM t1 ORDER BY a")
        assert tables(r) == {"t1": ["a", "b"]}

    def test_order_by_desc(self):
        r = extract("SELECT a FROM t1 ORDER BY b DESC")
        assert tables(r) == {"t1": ["a", "b"]}

    def test_group_by_and_order_by(self):
        r = extract(
            "SELECT a, COUNT(b) FROM t1 GROUP BY a ORDER BY COUNT(b)"
        )
        assert tables(r) == {"t1": ["a", "b"]}


# ====================================================================
# 5. DISTINCT
# ====================================================================


class TestDistinct:
    def test_select_distinct(self):
        r = extract("SELECT DISTINCT a, b FROM t1")
        assert tables(r) == {"t1": ["a", "b"]}

    def test_select_distinct_with_where(self):
        r = extract("SELECT DISTINCT a FROM t1 WHERE b = 1")
        assert tables(r) == {"t1": ["a", "b"]}


# ====================================================================
# 6. Aggregate functions
# ====================================================================


class TestAggregates:
    def test_count(self):
        r = extract("SELECT COUNT(a) FROM t1")
        assert tables(r) == {"t1": ["a"]}

    def test_sum(self):
        r = extract("SELECT SUM(a) FROM t1")
        assert tables(r) == {"t1": ["a"]}

    def test_avg(self):
        r = extract("SELECT AVG(a) FROM t1")
        assert tables(r) == {"t1": ["a"]}

    def test_min_max(self):
        r = extract("SELECT MIN(a), MAX(b) FROM t1")
        assert tables(r) == {"t1": ["a", "b"]}

    def test_count_star(self):
        """COUNT(*) should not produce any column reference."""
        r = extract("SELECT COUNT(*) FROM t1")
        assert tables(r) == {"t1": []}

    def test_nested_function(self):
        r = extract("SELECT COALESCE(a, b) FROM t1")
        assert tables(r) == {"t1": ["a", "b"]}


# ====================================================================
# 7. JOINs
# ====================================================================


class TestJoins:
    def test_inner_join_qualified(self):
        r = extract(
            "SELECT t1.a, t2.b FROM t1 JOIN t2 ON t1.id = t2.id"
        )
        assert tables(r) == {"t1": ["a", "id"], "t2": ["b", "id"]}
        assert ambiguous(r) == []

    def test_inner_join_unqualified_ambiguous(self):
        """Unqualified columns with multiple tables → ambiguous."""
        r = extract(
            "SELECT a, b FROM t1 JOIN t2 ON t1.id = t2.id"
        )
        assert "a" in ambiguous(r)
        assert "b" in ambiguous(r)

    def test_left_join(self):
        r = extract(
            "SELECT t1.a, t2.b FROM t1 LEFT JOIN t2 ON t1.id = t2.id"
        )
        assert tables(r) == {"t1": ["a", "id"], "t2": ["b", "id"]}

    def test_right_join(self):
        r = extract(
            "SELECT t1.a, t2.b FROM t1 RIGHT JOIN t2 ON t1.id = t2.id"
        )
        assert tables(r) == {"t1": ["a", "id"], "t2": ["b", "id"]}

    def test_full_outer_join(self):
        r = extract(
            "SELECT t1.a, t2.b FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.id"
        )
        assert tables(r) == {"t1": ["a", "id"], "t2": ["b", "id"]}

    def test_cross_join(self):
        r = extract("SELECT t1.a, t2.b FROM t1 CROSS JOIN t2")
        assert tables(r) == {"t1": ["a"], "t2": ["b"]}

    def test_multiple_joins(self):
        r = extract(
            "SELECT t1.a, t2.b, t3.c "
            "FROM t1 "
            "JOIN t2 ON t1.id = t2.t1_id "
            "JOIN t3 ON t2.id = t3.t2_id"
        )
        assert tables(r) == {
            "t1": ["a", "id"],
            "t2": ["b", "id", "t1_id"],
            "t3": ["c", "t2_id"],
        }

    def test_join_with_alias(self):
        r = extract(
            "SELECT x.a, y.b FROM t1 AS x JOIN t2 AS y ON x.id = y.id"
        )
        # Aliases resolve back to real table names
        assert tables(r) == {"t1": ["a", "id"], "t2": ["b", "id"]}
        assert ambiguous(r) == []

    def test_join_mixed_qualified_unqualified(self):
        """Qualified cols go to their table; unqualified → ambiguous."""
        r = extract(
            "SELECT t1.a, b FROM t1 JOIN t2 ON t1.id = t2.id"
        )
        assert tables(r)["t1"] == ["a", "id"]
        assert "b" in ambiguous(r)


# ====================================================================
# 8. UNION / INTERSECT / EXCEPT
# ====================================================================


class TestSetOperations:
    def test_union(self):
        r = extract("SELECT a FROM t1 UNION SELECT b FROM t2")
        assert tables(r) == {"t1": ["a"], "t2": ["b"]}
        assert ambiguous(r) == []

    def test_union_all(self):
        r = extract("SELECT a FROM t1 UNION ALL SELECT b FROM t2")
        assert tables(r) == {"t1": ["a"], "t2": ["b"]}

    def test_intersect(self):
        r = extract("SELECT a FROM t1 INTERSECT SELECT b FROM t2")
        assert tables(r) == {"t1": ["a"], "t2": ["b"]}

    def test_except(self):
        r = extract("SELECT a FROM t1 EXCEPT SELECT b FROM t2")
        assert tables(r) == {"t1": ["a"], "t2": ["b"]}

    def test_union_three_branches(self):
        r = extract(
            "SELECT a FROM t1 UNION SELECT b FROM t2 UNION SELECT c FROM t3"
        )
        assert tables(r) == {"t1": ["a"], "t2": ["b"], "t3": ["c"]}

    def test_union_with_where(self):
        r = extract(
            "SELECT a FROM t1 WHERE a > 1 "
            "UNION "
            "SELECT b FROM t2 WHERE b < 10"
        )
        assert tables(r) == {"t1": ["a"], "t2": ["b"]}


# ====================================================================
# 9. Nested subqueries
# ====================================================================


class TestSubqueries:
    def test_subquery_in_where_in(self):
        r = extract(
            "SELECT a FROM t1 WHERE a IN (SELECT b FROM t2)"
        )
        assert tables(r) == {"t1": ["a"], "t2": ["b"]}

    def test_subquery_in_where_exists(self):
        r = extract(
            "SELECT a FROM t1 WHERE EXISTS "
            "(SELECT 1 FROM t2 WHERE t2.id = t1.id)"
        )
        assert "t1" in tables(r)
        assert "t2" in tables(r)
        assert "a" in tables(r)["t1"]

    def test_subquery_in_from(self):
        r = extract(
            "SELECT sub.x FROM (SELECT a AS x FROM t1) AS sub"
        )
        # Inner query sees t1.a; outer query sees sub.x
        assert "t1" in tables(r)
        assert "a" in tables(r)["t1"]
        assert "sub" in tables(r)
        assert "x" in tables(r)["sub"]

    def test_deeply_nested_subquery(self):
        r = extract(
            "SELECT a FROM t1 WHERE a IN "
            "(SELECT b FROM t2 WHERE b IN "
            "(SELECT c FROM t3))"
        )
        assert tables(r) == {"t1": ["a"], "t2": ["b"], "t3": ["c"]}

    def test_subquery_in_select_list(self):
        r = extract(
            "SELECT a, (SELECT MAX(b) FROM t2) AS mx FROM t1"
        )
        assert "t1" in tables(r)
        assert "a" in tables(r)["t1"]
        assert "t2" in tables(r)
        assert "b" in tables(r)["t2"]


# ====================================================================
# 10. Ambiguous columns
# ====================================================================


class TestAmbiguous:
    def test_no_ambiguity_single_table(self):
        r = extract("SELECT a FROM t1")
        assert ambiguous(r) == []

    def test_ambiguous_with_two_tables(self):
        r = extract(
            "SELECT a, b FROM t1 JOIN t2 ON t1.id = t2.id"
        )
        assert sorted(ambiguous(r)) == ["a", "b"]

    def test_partially_qualified(self):
        """Some cols qualified, some not → only unqualified are ambiguous."""
        r = extract(
            "SELECT t1.a, b FROM t1 JOIN t2 ON t1.id = t2.id"
        )
        assert "a" not in ambiguous(r)
        assert "b" in ambiguous(r)

    def test_all_qualified_no_ambiguity(self):
        r = extract(
            "SELECT t1.a, t2.b FROM t1 JOIN t2 ON t1.id = t2.id"
        )
        assert ambiguous(r) == []

    def test_ambiguous_in_where(self):
        r = extract(
            "SELECT t1.a FROM t1 JOIN t2 ON t1.id = t2.id WHERE b = 1"
        )
        assert "b" in ambiguous(r)

    def test_ambiguous_in_order_by(self):
        r = extract(
            "SELECT t1.a FROM t1 JOIN t2 ON t1.id = t2.id ORDER BY c"
        )
        assert "c" in ambiguous(r)


# ====================================================================
# 11. Table aliases
# ====================================================================


class TestAliases:
    def test_single_table_alias(self):
        r = extract("SELECT x.a FROM t1 AS x")
        assert tables(r) == {"t1": ["a"]}

    def test_join_aliases_resolve(self):
        r = extract(
            "SELECT x.a, y.b FROM t1 AS x JOIN t2 AS y ON x.id = y.id"
        )
        assert tables(r) == {"t1": ["a", "id"], "t2": ["b", "id"]}

    def test_alias_in_where(self):
        r = extract("SELECT x.a FROM t1 AS x WHERE x.b > 5")
        assert tables(r) == {"t1": ["a", "b"]}


# ====================================================================
# 12. Multiple tables without JOIN
# ====================================================================


class TestImplicitJoin:
    def test_cartesian_product(self):
        r = extract("SELECT t1.a, t2.b FROM t1, t2")
        assert tables(r) == {"t1": ["a"], "t2": ["b"]}

    def test_cartesian_unqualified_ambiguous(self):
        r = extract("SELECT a, b FROM t1, t2")
        assert sorted(ambiguous(r)) == ["a", "b"]

    def test_cartesian_with_where(self):
        r = extract(
            "SELECT t1.a, t2.b FROM t1, t2 WHERE t1.id = t2.id"
        )
        assert tables(r) == {"t1": ["a", "id"], "t2": ["b", "id"]}


# ====================================================================
# 13. Edge cases
# ====================================================================


class TestEdgeCases:
    def test_select_literal_only(self):
        """SELECT 1 – no tables, no columns."""
        r = extract("SELECT 1")
        assert tables(r) == {}
        assert ambiguous(r) == []

    def test_select_expression(self):
        r = extract("SELECT a + b FROM t1")
        assert tables(r) == {"t1": ["a", "b"]}

    def test_duplicate_column_refs(self):
        """Same column referenced multiple times → appears once."""
        r = extract("SELECT a, a FROM t1 WHERE a > 1")
        assert tables(r) == {"t1": ["a"]}

    def test_case_expression(self):
        r = extract(
            "SELECT CASE WHEN a > 0 THEN b ELSE c END FROM t1"
        )
        assert tables(r) == {"t1": ["a", "b", "c"]}

    def test_multiple_aggregates_same_column(self):
        r = extract("SELECT COUNT(a), SUM(a) FROM t1")
        assert tables(r) == {"t1": ["a"]}

    def test_select_star_with_join(self):
        """SELECT * with a JOIN: wildcard attributed to every table in scope."""
        r = extract("SELECT * FROM t1 JOIN t2 ON t1.id = t2.id")
        assert tables(r) == {"t1": ["*", "id"], "t2": ["*", "id"]}

    def test_qualified_wildcard(self):
        """SELECT t1.* should attribute * only to that table."""
        r = extract("SELECT t1.* FROM t1")
        assert tables(r) == {"t1": ["*"]}

    def test_qualified_wildcard_with_join(self):
        """SELECT t1.* in a JOIN only attributes * to t1."""
        r = extract(
            "SELECT t1.*, t2.b FROM t1 JOIN t2 ON t1.id = t2.id"
        )
        assert tables(r) == {"t1": ["*", "id"], "t2": ["b", "id"]}

    def test_qualified_wildcard_with_alias(self):
        """SELECT x.* resolves the alias back to the real table."""
        r = extract("SELECT x.* FROM t1 AS x")
        assert tables(r) == {"t1": ["*"]}

    def test_wildcard_union(self):
        """SELECT * in each UNION branch attributes * to respective tables."""
        r = extract("SELECT * FROM t1 UNION SELECT * FROM t2")
        assert tables(r) == {"t1": ["*"], "t2": ["*"]}

    def test_wildcard_not_from_count_star(self):
        """COUNT(*) must NOT produce a wildcard column."""
        r = extract("SELECT COUNT(*) FROM t1")
        assert tables(r) == {"t1": []}
