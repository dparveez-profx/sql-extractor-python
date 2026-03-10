"""
Integration tests for the FastAPI ``/extract`` endpoint.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


class TestExtractEndpoint:
    def test_simple_query(self):
        resp = client.post("/extract", json={"query": "SELECT a FROM t1"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["tables"] == {"t1": ["a"]}
        assert data["ambiguous"] == []

    def test_complex_join(self):
        resp = client.post(
            "/extract",
            json={
                "query": (
                    "SELECT t1.a, t2.b FROM t1 "
                    "JOIN t2 ON t1.id = t2.id "
                    "WHERE t1.c > 5"
                )
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["tables"]["t1"] == ["a", "c", "id"]
        assert data["tables"]["t2"] == ["b", "id"]

    def test_union_query(self):
        resp = client.post(
            "/extract",
            json={"query": "SELECT a FROM t1 UNION SELECT b FROM t2"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["tables"] == {"t1": ["a"], "t2": ["b"]}

    def test_ambiguous_columns(self):
        resp = client.post(
            "/extract",
            json={
                "query": "SELECT a, b FROM t1 JOIN t2 ON t1.id = t2.id"
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert sorted(data["ambiguous"]) == ["a", "b"]

    def test_invalid_sql_returns_400(self):
        resp = client.post(
            "/extract",
            json={"query": "THIS IS NOT SQL AT ALL !!"},
        )
        assert resp.status_code == 400
        assert "detail" in resp.json()

    def test_empty_query_returns_422(self):
        """Pydantic enforces min_length=1 → 422."""
        resp = client.post("/extract", json={"query": ""})
        assert resp.status_code == 422

    def test_missing_query_field_returns_422(self):
        resp = client.post("/extract", json={})
        assert resp.status_code == 422

    def test_subquery(self):
        resp = client.post(
            "/extract",
            json={
                "query": "SELECT a FROM t1 WHERE a IN (SELECT b FROM t2)"
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["tables"] == {"t1": ["a"], "t2": ["b"]}

    def test_response_schema_keys(self):
        """Every successful response has exactly 'tables' and 'ambiguous'."""
        resp = client.post("/extract", json={"query": "SELECT 1"})
        assert resp.status_code == 200
        assert set(resp.json().keys()) == {"tables", "ambiguous"}
