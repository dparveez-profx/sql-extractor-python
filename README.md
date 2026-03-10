# SQL Extractor

A FastAPI application that accepts a SQL query and extracts the **tables** and **columns** referenced in it, using [mo-sql-parsing](https://github.com/klahnakoski/mo-sql-parsing).

## Features

- Extracts table → column mappings from SQL queries
- Handles a wide range of SQL constructs:
  - `SELECT`, `FROM`, `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`
  - `LIKE` / `NOT LIKE`
  - `DISTINCT`
  - Aggregate functions (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`)
  - All JOIN types (INNER, LEFT, RIGHT, FULL OUTER, CROSS)
  - `UNION` / `UNION ALL` / `INTERSECT` / `EXCEPT`
  - Nested subqueries (in `WHERE`, `FROM`, `SELECT` list)
  - Table aliases (resolved back to real table names)
- Columns that can't be unambiguously attributed to a table are reported under `"ambiguous"`
- **Optional schema-based disambiguation** — provide your real database
  schema (`table → columns`) and unqualified columns are automatically
  resolved to the correct table

## Quick Start

```bash
# Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -e ".[dev]"

# Run the server
uvicorn app.main:app --reload

# Run the tests
pytest -v
```

## API

### `POST /extract`

#### Request fields

| Field | Type | Required | Description |
|---|---|---|---|
| `query` | `string` | **yes** | The SQL query to analyse |
| `schema_map` | `object` | no | Real database schema for disambiguation — `{table: [col, …]}` |

---

#### Example 1 — Basic extraction (no schema)

**Request:**

```json
{
  "query": "SELECT t1.a, t2.b FROM t1 JOIN t2 ON t1.id = t2.id WHERE t1.c > 5"
}
```

**Response (200):**

```json
{
  "tables": {
    "t1": ["a", "c", "id"],
    "t2": ["b", "id"]
  },
  "ambiguous": []
}
```

---

#### Example 2 — Ambiguous columns (no schema)

When columns are unqualified and multiple tables are in scope, they land
in `"ambiguous"`:

**Request:**

```json
{
  "query": "SELECT a, b FROM t1 JOIN t2 ON t1.id = t2.id"
}
```

**Response (200):**

```json
{
  "tables": {
    "t1": ["id"],
    "t2": ["id"]
  },
  "ambiguous": ["a", "b"]
}
```

---

#### Example 3 — Schema-based disambiguation (resolved)

Provide `schema_map` so the extractor knows which table owns which
column.  Columns unique to a single in-scope table are resolved
automatically:

**Request:**

```json
{
  "query": "SELECT name, total FROM users JOIN orders ON users.id = orders.user_id WHERE email LIKE '%@example.com' ORDER BY total DESC",
  "schema_map": {
    "users":  ["id", "name", "email"],
    "orders": ["id", "user_id", "total"]
  }
}
```

**Response (200):**

```json
{
  "tables": {
    "orders": ["total", "user_id"],
    "users":  ["email", "id", "name"]
  },
  "ambiguous": []
}
```

`name` is only in `users`, `total` and `user_id` are only in `orders`,
and `email` is only in `users` — so every column is resolved.

---

#### Example 4 — Schema-based disambiguation (error)

If an unqualified column exists in **multiple** in-scope tables
according to the schema, the request fails with a 400:

**Request:**

```json
{
  "query": "SELECT id FROM users JOIN orders ON users.id = orders.user_id",
  "schema_map": {
    "users":  ["id", "name", "email"],
    "orders": ["id", "user_id", "total"]
  }
}
```

**Response (400):**

```json
{
  "detail": "Column 'id' is ambiguous — present in tables: orders, users",
  "column": "id",
  "tables": ["orders", "users"]
}
```

---

#### Example 5 — Partial schema (unknown columns stay ambiguous)

If the schema doesn't cover every table, columns that can't be found in
any schema table remain in `"ambiguous"`:

**Request:**

```json
{
  "query": "SELECT name, total FROM users JOIN orders ON users.id = orders.user_id",
  "schema_map": {
    "users": ["id", "name", "email"]
  }
}
```

**Response (200):**

```json
{
  "tables": {
    "orders": ["user_id"],
    "users":  ["id", "name"]
  },
  "ambiguous": ["total"]
}
```

`name` resolves to `users` (it's in the schema), but `total` isn't found
in any schema table so it stays ambiguous.

---

### Error Handling

| Status | Reason |
|--------|--------|
| 200    | Successful extraction |
| 400    | SQL could not be parsed **or** a column is ambiguous across multiple schema tables |
| 422    | Invalid request body (missing or empty `query`) |
