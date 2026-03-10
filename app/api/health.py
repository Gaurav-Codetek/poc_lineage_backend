from datetime import datetime
from typing import Any

from fastapi import APIRouter, HTTPException, Query, status
from pydantic import BaseModel

from app.db.databricks import DatabricksQueryError
from app.services.health_service import (
    load_column_health,
    load_complete_column_health,
    load_complete_table_health,
    load_table_health,
)

router = APIRouter()


class ErrorResponse(BaseModel):
    detail: str


class TableHealthSignal(BaseModel):
    signal_id: str
    table_catalog: str
    table_schema: str
    table_name: str
    full_table_name: str
    row_count: int | None = None
    column_count: int | None = None
    null_ratio: float | None = None
    uniqueness_ratio: float | None = None
    schema_hash: str | None = None
    freshness_timestamp: datetime | str | None = None
    pipeline_name: str | None = None
    status: str | None = None
    valid_from: datetime | str | None = None
    valid_to: datetime | str | None = None
    is_current: bool | None = None


class TableHealthListResponse(BaseModel):
    count: int
    data: list[TableHealthSignal]


class ColumnHealthSignal(BaseModel):
    signal_id: str
    table_catalog: str
    table_schema: str
    table_name: str
    column_name: str
    null_ratio: float | None = None
    uniqueness_ratio: float | None = None
    min_value: Any | None = None
    max_value: Any | None = None
    data_type: str | None = None
    timestamp: datetime | str | None = None


class ColumnHealthListResponse(BaseModel):
    count: int
    data: list[ColumnHealthSignal]


@router.get(
    "/table",
    response_model=TableHealthListResponse,
    responses={
        503: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
    tags=["health"],
)
def get_complete_table_health(
    is_current: bool | None = Query(
        default=None,
        description="Optional filter by current snapshot state.",
    )
) -> TableHealthListResponse:
    try:
        rows = load_complete_table_health(is_current=is_current)
    except DatabricksQueryError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Unable to fetch table observability signals from Databricks.",
        )

    signals = [TableHealthSignal(**row) for row in rows]
    return TableHealthListResponse(count=len(signals), data=signals)


@router.get(
    "/table/{catalog}.{schema}.{table}",
    response_model=TableHealthListResponse,
    responses={
        404: {"model": ErrorResponse},
        503: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
    tags=["health"],
)
def get_table_health(
    catalog: str,
    schema: str,
    table: str,
    is_current: bool | None = Query(
        default=None,
        description="Optional filter by current snapshot state.",
    ),
) -> TableHealthListResponse:
    try:
        rows = load_table_health(catalog, schema, table, is_current=is_current)
    except DatabricksQueryError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Unable to fetch table observability signals from Databricks.",
        )

    if not rows:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Table observability signals not found for {catalog}.{schema}.{table}.",
        )

    signals = [TableHealthSignal(**row) for row in rows]
    return TableHealthListResponse(count=len(signals), data=signals)


@router.get(
    "/column",
    response_model=ColumnHealthListResponse,
    responses={
        503: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
    tags=["health"],
)
def get_complete_column_health() -> ColumnHealthListResponse:
    try:
        rows = load_complete_column_health()
    except DatabricksQueryError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Unable to fetch column observability signals from Databricks.",
        )

    signals = [ColumnHealthSignal(**row) for row in rows]
    return ColumnHealthListResponse(count=len(signals), data=signals)


@router.get(
    "/column/{catalog}.{schema}.{table}",
    response_model=ColumnHealthListResponse,
    responses={
        404: {"model": ErrorResponse},
        503: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
    tags=["health"],
)
def get_column_health(
    catalog: str,
    schema: str,
    table: str,
) -> ColumnHealthListResponse:
    try:
        rows = load_column_health(catalog, schema, table)
    except DatabricksQueryError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Unable to fetch column observability signals from Databricks.",
        )

    if not rows:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Column observability signals not found for {catalog}.{schema}.{table}.",
        )

    signals = [ColumnHealthSignal(**row) for row in rows]
    return ColumnHealthListResponse(count=len(signals), data=signals)
