"""
FastAPI application – exposes a single ``POST /extract`` endpoint that
accepts a SQL query string and returns the extracted tables, columns,
and any ambiguous column references.
"""

from __future__ import annotations

from fastapi import FastAPI
from pydantic import BaseModel, Field

from app.extractor import extract

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
        json_schema_extra={"examples": ["SELECT a, b FROM t1 WHERE a > 1"]},
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
        result = extract(body.query)
    except Exception as exc:
        from fastapi.responses import JSONResponse

        return JSONResponse(
            status_code=400,
            content={"detail": f"Failed to parse SQL: {exc}"},
        )
    return SQLResponse(**result)
