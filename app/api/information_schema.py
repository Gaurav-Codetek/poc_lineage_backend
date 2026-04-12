from datetime import datetime

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, ConfigDict, Field

from app.db.databricks import DatabricksQueryError
from app.services.information_schema_service import (
    EntityNotFoundError,
    load_catalog_metadata,
    load_schema_metadata,
    load_table_metadata,
)

router = APIRouter()


class ErrorResponse(BaseModel):
    detail: str


class CatalogInfo(BaseModel):
    catalog_name: str
    catalog_owner: str | None = None
    created_at: datetime | str | None = None
    created_by: str | None = None
    last_altered_at: datetime | str | None = None
    last_altered_by: str | None = None


class CatalogSchemaInfo(BaseModel):
    schema_name: str
    schema_owner: str | None = None
    created_at: datetime | str | None = None
    last_altered_at: datetime | str | None = None
    last_altered_by: str | None = None


class CatalogMetadataResponse(BaseModel):
    catalog: CatalogInfo
    schema_count: int
    schemas: list[CatalogSchemaInfo]


class SchemaInfo(BaseModel):
    catalog_name: str
    schema_name: str
    schema_owner: str | None = None
    created_at: datetime | str | None = None
    last_altered_at: datetime | str | None = None
    last_altered_by: str | None = None


class SchemaTableInfo(BaseModel):
    table_catalog: str
    table_schema: str
    table_name: str
    table_type: str | None = None
    table_owner: str | None = None
    comment: str | None = None
    created_at: datetime | str | None = None
    created_by: str | None = None


class SchemaMetadataResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    schema_details: SchemaInfo = Field(alias="schema")
    table_count: int
    tables: list[SchemaTableInfo]


class TableInfo(BaseModel):
    table_catalog: str
    table_schema: str
    table_name: str
    table_type: str | None = None
    table_owner: str | None = None
    comment: str | None = None
    created_at: datetime | str | None = None
    created_by: str | None = None


class TableColumnInfo(BaseModel):
    table_catalog: str
    table_schema: str
    table_name: str
    column_name: str
    comment: str | None = None
    full_data_type: str | None = None
    data_type: str | None = None


class TableMetadataResponse(BaseModel):
    table: TableInfo
    column_count: int
    columns: list[TableColumnInfo]


@router.get(
    "/catalog/{catalog}",
    response_model=CatalogMetadataResponse,
    responses={
        404: {"model": ErrorResponse},
        503: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
    tags=["information-schema"],
)
def get_catalog_metadata(catalog: str) -> CatalogMetadataResponse:
    try:
        payload = load_catalog_metadata(catalog)
    except EntityNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))
    except DatabricksQueryError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Unable to fetch catalog metadata from Databricks Information Schema.",
        )

    return CatalogMetadataResponse(**payload)


@router.get(
    "/schema/{catalog}.{schema}",
    response_model=SchemaMetadataResponse,
    responses={
        404: {"model": ErrorResponse},
        503: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
    tags=["information-schema"],
)
def get_schema_metadata(catalog: str, schema: str) -> SchemaMetadataResponse:
    try:
        payload = load_schema_metadata(catalog, schema)
    except EntityNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))
    except DatabricksQueryError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Unable to fetch schema metadata from Databricks Information Schema.",
        )

    return SchemaMetadataResponse(**payload)


@router.get(
    "/table/{catalog}.{schema}.{table}",
    response_model=TableMetadataResponse,
    responses={
        404: {"model": ErrorResponse},
        503: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
    tags=["information-schema"],
)
def get_table_metadata(catalog: str, schema: str, table: str) -> TableMetadataResponse:
    try:
        payload = load_table_metadata(catalog, schema, table)
    except EntityNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))
    except DatabricksQueryError:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Unable to fetch table metadata from Databricks Information Schema.",
        )

    return TableMetadataResponse(**payload)
