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

**Request body:**

```json
{
  "query": "SELECT t1.a, t2.b FROM t1 JOIN t2 ON t1.id = t2.id WHERE t1.c > 5"
}
```

**Response:**

```json
{
  "tables": {
    "t1": ["a", "c", "id"],
    "t2": ["b", "id"]
  },
  "ambiguous": []
}
```

Columns that cannot be attributed to a single table (unqualified columns when
multiple tables are in scope) appear in `"ambiguous"`:

```json
{
  "query": "SELECT a, b FROM t1 JOIN t2 ON t1.id = t2.id"
}
```

```json
{
  "tables": {
    "t1": ["id"],
    "t2": ["id"]
  },
  "ambiguous": ["a", "b"]
}
```

### Error Handling

| Status | Reason |
|--------|--------|
| 200    | Successful extraction |
| 400    | SQL could not be parsed |
| 422    | Invalid request body (missing or empty `query`) |
