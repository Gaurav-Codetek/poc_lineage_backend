from typing import Any

from fastapi import APIRouter, Body, HTTPException, Query

from app.services.dq_service import build_run_response, build_suggestion_response

router = APIRouter()


def _run_error_response(error: Exception) -> dict[str, Any]:
    if isinstance(error, FileNotFoundError):
        return {
            "error": str(error),
            "hint": "Pass catalog_json_path or metadata_path to a valid JSON/YAML metadata file.",
        }
    if isinstance(error, KeyError):
        return {
            "error": str(error),
            "hint": "Ensure the metadata source contains the requested catalog.schema.table.",
        }
    if isinstance(error, ValueError):
        return {
            "error": str(error),
            "hint": "Pass table_fqn in catalog.schema.table format, or supply metadata_path for a single-table file.",
        }
    raise HTTPException(status_code=500, detail=str(error))


@router.get("")
@router.get("/", include_in_schema=False)
def get_dq_suggestions_by_query(
    table_fqn: str | None = Query(default=None, description="catalog.schema.table"),
    catalog_json_path: str | None = Query(default=None),
    metadata_path: str | None = Query(default=None),
) -> dict[str, Any]:
    try:
        return build_suggestion_response(
            table_fqn=table_fqn,
            catalog_json_path=catalog_json_path,
            metadata_path=metadata_path,
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/{catalog}.{schema}.{table}")
@router.get("/{catalog}.{schema}.{table}/", include_in_schema=False)
def get_dq_suggestions(catalog: str, schema: str, table: str) -> dict[str, Any]:
    try:
        return build_suggestion_response(catalog, schema, table)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/run")
@router.post("/run/", include_in_schema=False)
async def run_dq_checks_by_query(
    payload: dict[str, Any] | None = Body(default=None, description="Payload with selections"),
    table_fqn: str | None = Query(default=None, description="catalog.schema.table"),
    catalog_json_path: str | None = Query(default=None),
    metadata_path: str | None = Query(default=None),
) -> dict[str, Any]:
    try:
        return build_run_response(
            None,
            None,
            None,
            payload or {},
            table_fqn=table_fqn,
            catalog_json_path=catalog_json_path,
            metadata_path=metadata_path,
        )
    except (FileNotFoundError, KeyError, ValueError) as exc:
        return _run_error_response(exc)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.post("/run/{catalog}.{schema}.{table}")
@router.post("/run/{catalog}.{schema}.{table}/", include_in_schema=False)
async def run_dq_checks(
    catalog: str,
    schema: str,
    table: str,
    payload: dict[str, Any] = Body(..., description="Payload with selections"),
) -> dict[str, Any]:
    try:
        return build_run_response(catalog, schema, table, payload)
    except (FileNotFoundError, KeyError, ValueError) as exc:
        return _run_error_response(exc)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
