from typing import Any

from fastapi import APIRouter, Body, HTTPException

from app.services.dq_service import build_run_response, build_suggestion_response

router = APIRouter()


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
    except FileNotFoundError as exc:
        return {
            "error": str(exc),
            "hint": "Place documentation.json under app/data and ensure it contains metadata for the requested table.",
        }
    except KeyError as exc:
        return {
            "error": str(exc),
            "hint": "Ensure documentation.json contains metadata for the requested catalog.schema.table.",
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
