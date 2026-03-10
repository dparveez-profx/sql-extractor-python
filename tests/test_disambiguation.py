"""
Tests for schema-based column disambiguation.

When a ``schema_map`` (table → columns) is provided, unqualified columns
that would otherwise be ambiguous are resolved by looking up which
in-scope table actually owns them.  If a column still belongs to
multiple tables an ``AmbiguousColumnError`` is raised.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.extractor import AmbiguousColumnError, extract
from app.main import app

client = TestClient(app)


# ── Helpers ─────────────────────────────────────────────────────────


def tables(result: dict) -> dict[str, list[str]]:
    return result["tables"]


def ambiguous(result: dict) -> list[str]:
    return result["ambiguous"]


# ====================================================================
# 1. Core extractor – successful disambiguation
# ====================================================================


class TestDisambiguationResolves:
    """Column exists in exactly one in-scope table → resolved."""

    SCHEMA = {
        "users": ["id", "name", "email"],
        "orders": ["id", "user_id", "total"],
    }

    def test_unique_columns_resolved(self):
        """'name' only in users, 'total' only in orders → resolved."""
        r = extract(
            "SELECT name, total FROM users JOIN orders ON users.id = orders.user_id",
            schema=self.SCHEMA,
        )
        assert tables(r)["users"] == ["id", "name"]
        assert tables(r)["orders"] == ["total", "user_id"]
        assert ambiguous(r) == []

    def test_single_unique_column(self):
        r = extract(
            "SELECT email FROM users JOIN orders ON users.id = orders.user_id",
            schema=self.SCHEMA,
        )
        assert "email" in tables(r)["users"]
        assert ambiguous(r) == []

    def test_where_clause_disambiguated(self):
        r = extract(
            "SELECT users.id FROM users JOIN orders ON users.id = orders.user_id "
            "WHERE total > 100",
            schema=self.SCHEMA,
        )
        assert "total" in tables(r)["orders"]
        assert ambiguous(r) == []

    def test_order_by_disambiguated(self):
        r = extract(
            "SELECT users.id FROM users JOIN orders ON users.id = orders.user_id "
            "ORDER BY name",
            schema=self.SCHEMA,
        )
        assert "name" in tables(r)["users"]
        assert ambiguous(r) == []

    def test_group_by_disambiguated(self):
        r = extract(
            "SELECT name, SUM(total) "
            "FROM users JOIN orders ON users.id = orders.user_id "
            "GROUP BY name",
            schema=self.SCHEMA,
        )
        assert "name" in tables(r)["users"]
        assert "total" in tables(r)["orders"]
        assert ambiguous(r) == []

    def test_having_disambiguated(self):
        r = extract(
            "SELECT name, SUM(total) "
            "FROM users JOIN orders ON users.id = orders.user_id "
            "GROUP BY name HAVING SUM(total) > 500",
            schema=self.SCHEMA,
        )
        assert "name" in tables(r)["users"]
        assert "total" in tables(r)["orders"]
        assert ambiguous(r) == []

    def test_mixed_qualified_and_disambiguated(self):
        """Qualified columns resolve normally; unqualified use schema."""
        r = extract(
            "SELECT users.id, total FROM users JOIN orders ON users.id = orders.user_id",
            schema=self.SCHEMA,
        )
        assert "id" in tables(r)["users"]
        assert "total" in tables(r)["orders"]
        assert ambiguous(r) == []

    def test_three_table_join(self):
        schema = {
            "a": ["id", "x"],
            "b": ["id", "a_id", "y"],
            "c": ["id", "b_id", "z"],
        }
        r = extract(
            "SELECT x, y, z FROM a "
            "JOIN b ON a.id = b.a_id "
            "JOIN c ON b.id = c.b_id",
            schema=schema,
        )
        assert "x" in tables(r)["a"]
        assert "y" in tables(r)["b"]
        assert "z" in tables(r)["c"]
        assert ambiguous(r) == []

    def test_alias_tables_resolved_via_schema(self):
        """Aliases resolve to real names, then schema lookup works."""
        r = extract(
            "SELECT name, total "
            "FROM users AS u JOIN orders AS o ON u.id = o.user_id",
            schema=self.SCHEMA,
        )
        assert "name" in tables(r)["users"]
        assert "total" in tables(r)["orders"]
        assert ambiguous(r) == []


# ====================================================================
# 2. Core extractor – AmbiguousColumnError
# ====================================================================


class TestDisambiguationErrors:
    """Column exists in multiple in-scope tables → error."""

    SCHEMA = {
        "users": ["id", "name", "email"],
        "orders": ["id", "user_id", "total"],
    }

    def test_shared_column_raises(self):
        """'id' exists in both tables → AmbiguousColumnError."""
        with pytest.raises(AmbiguousColumnError) as exc_info:
            extract(
                "SELECT id FROM users JOIN orders ON users.id = orders.user_id",
                schema=self.SCHEMA,
            )
        assert exc_info.value.column == "id"
        assert sorted(exc_info.value.tables) == ["orders", "users"]

    def test_error_message_contains_details(self):
        with pytest.raises(AmbiguousColumnError, match="id"):
            extract(
                "SELECT id FROM users JOIN orders ON users.id = orders.user_id",
                schema=self.SCHEMA,
            )

    def test_shared_column_in_where_raises(self):
        with pytest.raises(AmbiguousColumnError) as exc_info:
            extract(
                "SELECT users.name FROM users JOIN orders ON users.id = orders.user_id "
                "WHERE id = 5",
                schema=self.SCHEMA,
            )
        assert exc_info.value.column == "id"

    def test_shared_column_in_order_by_raises(self):
        with pytest.raises(AmbiguousColumnError):
            extract(
                "SELECT users.name FROM users JOIN orders ON users.id = orders.user_id "
                "ORDER BY id",
                schema=self.SCHEMA,
            )

    def test_multiple_shared_columns_raises_on_first(self):
        """When multiple columns are ambiguous, the first one encountered raises."""
        schema = {
            "t1": ["a", "b", "shared"],
            "t2": ["c", "d", "shared"],
        }
        with pytest.raises(AmbiguousColumnError) as exc_info:
            extract(
                "SELECT shared FROM t1 JOIN t2 ON t1.a = t2.c",
                schema=schema,
            )
        assert exc_info.value.column == "shared"


# ====================================================================
# 3. Core extractor – column not in schema (stays ambiguous)
# ====================================================================


class TestDisambiguationUnknownColumn:
    """Column not in any schema table → remains in ambiguous list."""

    SCHEMA = {
        "users": ["id", "name"],
        "orders": ["id", "total"],
    }

    def test_unknown_column_stays_ambiguous(self):
        r = extract(
            "SELECT unknown_col FROM users JOIN orders ON users.id = orders.id",
            schema=self.SCHEMA,
        )
        assert "unknown_col" in ambiguous(r)

    def test_mix_of_resolved_and_unknown(self):
        r = extract(
            "SELECT name, unknown_col FROM users JOIN orders ON users.id = orders.id",
            schema=self.SCHEMA,
        )
        assert "name" in tables(r)["users"]
        assert "unknown_col" in ambiguous(r)


# ====================================================================
# 4. Core extractor – no schema (backward compatibility)
# ====================================================================


class TestNoSchema:
    """Without a schema, behaviour is unchanged from before."""

    def test_no_schema_ambiguous_as_before(self):
        r = extract(
            "SELECT a, b FROM t1 JOIN t2 ON t1.id = t2.id",
        )
        assert sorted(ambiguous(r)) == ["a", "b"]

    def test_no_schema_single_table_resolves(self):
        r = extract("SELECT a, b FROM t1")
        assert tables(r) == {"t1": ["a", "b"]}
        assert ambiguous(r) == []

    def test_explicit_none_schema(self):
        r = extract(
            "SELECT a FROM t1 JOIN t2 ON t1.id = t2.id",
            schema=None,
        )
        assert "a" in ambiguous(r)


# ====================================================================
# 5. Core extractor – schema with subqueries / set operations
# ====================================================================


class TestDisambiguationWithSubqueries:
    SCHEMA = {
        "users": ["id", "name", "email"],
        "orders": ["id", "user_id", "total"],
    }

    def test_union_branches_disambiguated_independently(self):
        """Each UNION branch has its own scope; schema applies per-scope."""
        schema = {"t1": ["a", "x"], "t2": ["b", "x"]}
        r = extract(
            "SELECT a FROM t1 UNION SELECT b FROM t2",
            schema=schema,
        )
        assert tables(r) == {"t1": ["a"], "t2": ["b"]}

    def test_subquery_disambiguation(self):
        """Subqueries get their own scope; schema helps in each."""
        schema = {
            "t1": ["a", "b"],
            "t2": ["b", "c"],
            "t3": ["c", "d"],
        }
        r = extract(
            "SELECT a FROM t1 WHERE a IN (SELECT c FROM t3)",
            schema=schema,
        )
        assert tables(r) == {"t1": ["a"], "t3": ["c"]}
        assert ambiguous(r) == []


# ====================================================================
# 6. API integration – schema_map parameter
# ====================================================================


class TestAPIDisambiguation:
    SCHEMA = {
        "users": ["id", "name", "email"],
        "orders": ["id", "user_id", "total"],
    }

    def test_api_without_schema_map(self):
        """Omitting schema_map works exactly as before."""
        resp = client.post(
            "/extract",
            json={"query": "SELECT a FROM t1"},
        )
        assert resp.status_code == 200
        assert resp.json()["tables"] == {"t1": ["a"]}

    def test_api_with_null_schema_map(self):
        resp = client.post(
            "/extract",
            json={"query": "SELECT a FROM t1", "schema_map": None},
        )
        assert resp.status_code == 200

    def test_api_disambiguation_resolves(self):
        resp = client.post(
            "/extract",
            json={
                "query": (
                    "SELECT name, total "
                    "FROM users JOIN orders ON users.id = orders.user_id"
                ),
                "schema_map": self.SCHEMA,
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert "name" in data["tables"]["users"]
        assert "total" in data["tables"]["orders"]
        assert data["ambiguous"] == []

    def test_api_disambiguation_error_returns_400(self):
        resp = client.post(
            "/extract",
            json={
                "query": (
                    "SELECT id "
                    "FROM users JOIN orders ON users.id = orders.user_id"
                ),
                "schema_map": self.SCHEMA,
            },
        )
        assert resp.status_code == 400
        data = resp.json()
        assert "detail" in data
        assert data["column"] == "id"
        assert sorted(data["tables"]) == ["orders", "users"]

    def test_api_unknown_column_stays_ambiguous(self):
        resp = client.post(
            "/extract",
            json={
                "query": (
                    "SELECT mystery "
                    "FROM users JOIN orders ON users.id = orders.user_id"
                ),
                "schema_map": self.SCHEMA,
            },
        )
        assert resp.status_code == 200
        assert "mystery" in resp.json()["ambiguous"]

    def test_api_complex_disambiguation(self):
        """Full scenario: qualified + disambiguated + ON columns."""
        resp = client.post(
            "/extract",
            json={
                "query": (
                    "SELECT users.id, name, total "
                    "FROM users "
                    "JOIN orders ON users.id = orders.user_id "
                    "WHERE email LIKE '%@example.com' "
                    "ORDER BY total DESC"
                ),
                "schema_map": self.SCHEMA,
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert sorted(data["tables"]["users"]) == ["email", "id", "name"]
        assert sorted(data["tables"]["orders"]) == ["total", "user_id"]
        assert data["ambiguous"] == []

    def test_api_schema_map_empty_dict(self):
        """Empty schema_map → no disambiguation possible, columns stay ambiguous."""
        resp = client.post(
            "/extract",
            json={
                "query": "SELECT a FROM t1 JOIN t2 ON t1.id = t2.id",
                "schema_map": {},
            },
        )
        assert resp.status_code == 200
        assert "a" in resp.json()["ambiguous"]

    def test_api_schema_map_partial(self):
        """Schema only covers some tables; uncovered columns stay ambiguous."""
        resp = client.post(
            "/extract",
            json={
                "query": (
                    "SELECT name, total "
                    "FROM users JOIN orders ON users.id = orders.user_id"
                ),
                "schema_map": {"users": ["id", "name", "email"]},
                # orders not in schema
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        # name resolves to users
        assert "name" in data["tables"]["users"]
        # total not found in any schema table → ambiguous
        assert "total" in data["ambiguous"]
