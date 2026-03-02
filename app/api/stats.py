import os
from typing import Annotated

from fastapi import APIRouter, FastAPI, HTTPException, Path, Request, status
from pydantic import BaseModel

from app.db.databricks import DatabricksQueryError
from app.services.stats_service import fetch_freshness, fetch_profile, fetch_quality

SERVICE_NAME = "Data Discovery API"
SERVICE_VERSION = "1.0.0"


class ErrorResponse(BaseModel):
    detail: str


class HealthResponse(BaseModel):
    status: str
    service: str
    target_schema: str


class FreshnessResponse(BaseModel):
    last_modified_at: str | None = None
    latency_hours: float | None = None
    current_version: int | None = None
    last_documented_at: str | None = None


class QualityColumn(BaseModel):
    column_name: str
    data_type: str | None = None
    null_percentage: float | None = None
    distinct_count: int | None = None
    total_rows: int | None = None


class ProfileColumn(BaseModel):
    column_name: str
    data_type: str | None = None
    mean: float | None = None
    min_val: float | None = None
    p25: float | None = None
    median: float | None = None
    p75: float | None = None
    max_val: float | None = None


# app = FastAPI(title=SERVICE_NAME, version=SERVICE_VERSION)
router = APIRouter()

# @app.exception_handler(RequestValidationError)
# async def validation_exception_handler(
#     _request: Request, exc: RequestValidationError
# ) -> JSONResponse:
#     return JSONResponse(
#         status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
#         content={"detail": "Request validation failed", "errors": exc.errors()},
#     )


# @app.exception_handler(Exception)
# async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
#     LOGGER.exception(
#         "Unhandled exception for %s %s",
#         request.method,
#         request.url.path,
#         exc_info=exc,
#     )
#     return JSONResponse(
#         status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#         content={"detail": "Internal server error"},
#     )


# @app.on_event("startup")
# def on_startup() -> None:
#     LOGGER.info("Starting %s v%s", SERVICE_NAME, SERVICE_VERSION)
#     LOGGER.info("Target schema: %s", settings.target_schema)

#     if settings.startup_db_check:
#         try:
#             check_databricks_connectivity()
#             LOGGER.info("Databricks startup connectivity check passed.")
#         except DatabricksQueryError as exc:
#             LOGGER.error("Databricks startup connectivity check failed.")
#             raise RuntimeError(
#                 "Startup check failed: unable to connect to Databricks."
#             ) from exc


@router.get(
    "/freshness/{catalog}.{schema}.{table}",
    response_model=FreshnessResponse,
    responses={
        400: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
        503: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
    tags=["stats"],
)
def get_freshness(
    catalog: str, schema: str, table: str
) -> FreshnessResponse:

    try:
        row = fetch_freshness(catalog, schema, table)
    except DatabricksQueryError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Unable to fetch freshness metrics from Databricks.",
        )

    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Freshness data not found for {catalog}.{schema}.{table}",
        )

    return FreshnessResponse(**row)


@router.get(
    "/quality/{catalog}.{schema}.{table}",
    response_model=list[QualityColumn],
    responses={
        400: {"model": ErrorResponse},
        503: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
    tags=["stats"],
)
def get_quality(
    catalog: str, schema: str, table: str
) -> list[QualityColumn]:

    try:
        rows = fetch_quality(catalog, schema, table)
    except DatabricksQueryError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Unable to fetch quality metrics from Databricks.",
        )

    return [QualityColumn(**row) for row in rows]


@router.get(
    "/profile/{catalog}.{schema}.{table}",
    response_model=list[ProfileColumn],
    responses={
        400: {"model": ErrorResponse},
        503: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
    tags=["stats"],
)
def get_profile(
    catalog: str, schema: str, table: str
) -> list[ProfileColumn]:

    try:
        rows = fetch_profile(catalog, schema, table)
    except DatabricksQueryError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Unable to fetch profile metrics from Databricks.",
        )

    return [ProfileColumn(**row) for row in rows]
