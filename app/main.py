"""
FastAPI application – exposes a single ``POST /extract`` endpoint that
accepts a SQL query string and returns the extracted tables, columns,
and any ambiguous column references.
"""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.extractor import AmbiguousColumnError, extract

app = FastAPI(
    title="SQL Extractor",
    description="Extract table and column names from SQL queries.",
    version="0.1.0",
)


# ── Request / Response models ───────────────────────────────────────


class SQLRequest(BaseModel):
    query: str = Field(
        ...,
        min_length=1,
        description="The SQL query to analyse.",
        json_schema_extra={
            "examples": ["SELECT a, b FROM t1 WHERE a > 1"],
        },
    )
    schema_map: dict[str, list[str]] | None = Field(
        default=None,
        description=(
            "Optional real database schema used for column disambiguation. "
            "Mapping of table name → list of column names."
        ),
        json_schema_extra={
            "examples": [
                {
                    "users": ["id", "name", "email"],
                    "orders": ["id", "user_id", "total"],
                }
            ],
        },
    )


class SQLResponse(BaseModel):
    tables: dict[str, list[str]] = Field(
        ...,
        description="Mapping of table name → sorted list of columns.",
    )
    ambiguous: list[str] = Field(
        ...,
        description="Columns that could not be attributed to a single table.",
    )


class ErrorResponse(BaseModel):
    detail: str


# ── Endpoints ───────────────────────────────────────────────────────


@app.post(
    "/extract",
    response_model=SQLResponse,
    responses={400: {"model": ErrorResponse}},
    summary="Extract tables & columns from a SQL query",
)
def extract_sql(body: SQLRequest) -> SQLResponse:
    try:
        result = extract(body.query, schema=body.schema_map)
    except AmbiguousColumnError as exc:
        return JSONResponse(
            status_code=400,
            content={
                "detail": str(exc),
                "column": exc.column,
                "tables": exc.tables,
            },
        )
    except Exception as exc:
        return JSONResponse(
            status_code=400,
            content={"detail": f"Failed to parse SQL: {exc}"},
        )
    return SQLResponse(**result)
