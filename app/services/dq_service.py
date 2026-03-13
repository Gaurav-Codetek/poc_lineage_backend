import hashlib
import json
import os
import re
import threading
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from app.cache.dq_cache import load_dq_cache, save_dq_cache
from app.db.databricks import get_db_connection

try:
    from openai import AzureOpenAI
except Exception:
    AzureOpenAI = None


DOCUMENTATION_PATH = Path(__file__).resolve().parents[1] / "data" / "documentation.json"

RULE_ORDER = [
    "not_null",
    "no_blank_strings",
    "length_validation",
    "unique_values",
    "numeric_range_validation",
    "zero_value_check",
    "no_future_dates",
    "freshness_validation",
]

RULE_TITLES = {
    "not_null": "Not NULL",
    "no_blank_strings": "No Blank Strings",
    "length_validation": "Length Validation",
    "unique_values": "Uniqueness",
    "numeric_range_validation": "Numeric Range",
    "zero_value_check": "Zero Value Check",
    "no_future_dates": "No Future Dates",
    "freshness_validation": "Freshness Check",
}

_DQ_SUGGESTION_REFRESH_LOCK = threading.Lock()
_DQ_SUGGESTION_REFRESH_IN_PROGRESS: set[str] = set()

_LLM_RULES_DEF = """
Allowed DQ rules (use only these exact keys):
- not_null: Column should not be NULL when business rules say "required", "must", or equivalent.
- no_blank_strings: For STRING-like text, disallow empty/blank or whitespace-only values.
- length_validation: For STRING-like text with explicit length hints, enforce reasonable maximum length.
- unique_values: Use only when the description implies column-level uniqueness; do NOT assume for generic foreign keys.
- numeric_range_validation: For numeric fields with explicit constraints (e.g., >= 0, > 0, integer-only).
- zero_value_check: For numeric fields where 0 is suspicious/invalid per description.
- no_future_dates: For date/time semantics where future dates should not occur.
- freshness_validation: For lineage/reporting dates where recency is expected.
"""


def _normalize_column(column: dict[str, Any]) -> dict[str, Any] | None:
    column_name = column.get("column_name") or column.get("name")
    if not column_name:
        return None

    return {
        "column_name": column_name,
        "data_type": column.get("data_type") or column.get("type") or "",
        "description": column.get("description") or column.get("summary") or "",
    }


def _full_name_from_parts(catalog: str | None, schema: str | None, table_name: str | None) -> str | None:
    if not catalog or not schema or not table_name:
        return None
    return f"{catalog}.{schema}.{table_name}"


def _normalize_metadata_candidate(
    candidate: dict[str, Any],
    inherited_key: str | None = None,
    inherited_catalog: str | None = None,
    inherited_schema: str | None = None,
) -> dict[str, Any] | None:
    table_block = candidate.get("table") if isinstance(candidate.get("table"), dict) else None

    catalog = candidate.get("catalog") or inherited_catalog
    schema = candidate.get("schema") or inherited_schema
    table_name = candidate.get("table_name")

    if table_block:
        table_name = table_name or table_block.get("table_name")

    full_name = candidate.get("full_table_name")
    if not full_name and inherited_key and inherited_key.count(".") == 2:
        full_name = inherited_key

    if full_name and full_name.count(".") >= 2:
        parts = full_name.split(".")
        catalog = catalog or parts[0]
        schema = schema or parts[1]
        table_name = table_name or parts[-1]

    table_name = table_name or candidate.get("name")
    full_name = full_name or _full_name_from_parts(catalog, schema, table_name)

    raw_columns = None
    if table_block:
        raw_columns = table_block.get("columns")
    if raw_columns is None:
        raw_columns = candidate.get("columns")

    if not isinstance(raw_columns, list):
        return None

    normalized_columns = []
    for raw_column in raw_columns:
        if isinstance(raw_column, dict):
            normalized = _normalize_column(raw_column)
            if normalized:
                normalized_columns.append(normalized)

    if not normalized_columns or not catalog or not schema or not table_name:
        return None

    table_description = candidate.get("table_description") or ""
    if table_block:
        table_description = table_block.get("table_description") or table_description

    return {
        "catalog": catalog,
        "schema": schema,
        "table": {
            "table_name": table_name,
            "table_description": table_description,
            "columns": normalized_columns,
        },
        "full_table_name": full_name or _full_name_from_parts(catalog, schema, table_name),
    }


def _iter_metadata_candidates(
    node: Any,
    inherited_key: str | None = None,
    inherited_catalog: str | None = None,
    inherited_schema: str | None = None,
):
    if isinstance(node, dict):
        normalized = _normalize_metadata_candidate(
            node,
            inherited_key=inherited_key,
            inherited_catalog=inherited_catalog,
            inherited_schema=inherited_schema,
        )
        if normalized:
            yield normalized

        next_catalog = node.get("catalog") if isinstance(node.get("catalog"), str) else inherited_catalog
        next_schema = node.get("schema") if isinstance(node.get("schema"), str) else inherited_schema

        for key, value in node.items():
            next_key = key if isinstance(key, str) else inherited_key
            yield from _iter_metadata_candidates(
                value,
                inherited_key=next_key,
                inherited_catalog=next_catalog,
                inherited_schema=next_schema,
            )

    if isinstance(node, list):
        for item in node:
            yield from _iter_metadata_candidates(
                item,
                inherited_key=inherited_key,
                inherited_catalog=inherited_catalog,
                inherited_schema=inherited_schema,
            )


@lru_cache(maxsize=1)
def _load_documentation() -> Any:
    if not DOCUMENTATION_PATH.exists():
        raise FileNotFoundError(f"Documentation file not found: {DOCUMENTATION_PATH}")

    with DOCUMENTATION_PATH.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def clear_documentation_cache() -> None:
    _load_documentation.cache_clear()


def get_table_metadata(catalog: str, schema: str, table: str) -> dict[str, Any]:
    documentation = _load_documentation()
    target = f"{catalog}.{schema}.{table}".lower()

    for candidate in _iter_metadata_candidates(documentation):
        candidate_name = (candidate.get("full_table_name") or "").lower()
        if candidate_name == target:
            return {
                "catalog": candidate["catalog"],
                "schema": candidate["schema"],
                "table": candidate["table"],
            }

    raise KeyError(f"Metadata not found for {catalog}.{schema}.{table} in {DOCUMENTATION_PATH}")


def _static_suggest(dtype: str) -> List[str]:
    normalized_dtype = (dtype or "").upper()
    rules = ["not_null", "unique_values"]
    if normalized_dtype == "STRING":
        rules += ["no_blank_strings", "length_validation"]
    if normalized_dtype in {"LONG", "DOUBLE", "INTEGER"}:
        rules += ["numeric_range_validation", "zero_value_check"]
    if normalized_dtype in {"TIMESTAMP", "DATE"}:
        rules += ["no_future_dates", "freshness_validation"]
    return rules


def _build_suggestions_static(columns: List[Dict[str, Any]]) -> Tuple[Dict[str, List[str]], Dict[str, str]]:
    suggested_by_rule = {rule: [] for rule in RULE_ORDER}
    dtype_by_col = {}

    for column in columns:
        name = column["column_name"]
        dtype = column.get("data_type", "")
        dtype_by_col[name] = dtype
        for rule in _static_suggest(dtype):
            suggested_by_rule.setdefault(rule, []).append(name)

    suggested_by_rule = {rule: columns for rule, columns in suggested_by_rule.items() if columns}
    return suggested_by_rule, dtype_by_col


def _get_llm_client() -> Any | None:
    if AzureOpenAI is None:
        return None

    api_key = os.getenv("API_KEY") or os.getenv("AZURE_OPENAI_API_KEY")
    api_version = os.getenv("API_VERSION") or os.getenv("AZURE_OPENAI_API_VERSION")
    endpoint = os.getenv("ENDPOINT") or os.getenv("AZURE_OPENAI_ENDPOINT")

    if not api_key or not api_version or not endpoint:
        return None

    return AzureOpenAI(
        api_key=api_key,
        api_version=api_version,
        azure_endpoint=endpoint,
    )


def _safe_json_from_text(text: str) -> Optional[Dict[str, Any]]:
    if not text:
        return None

    try:
        return json.loads(text)
    except Exception:
        pass

    match = re.search(r"\{[\s\S]*\}", text)
    if match:
        try:
            return json.loads(match.group(0))
        except Exception:
            return None

    return None


def _sanitize_llm_mapping(
    raw: Dict[str, Any], valid_rules: List[str], valid_cols: List[str]
) -> Dict[str, List[str]]:
    output: Dict[str, List[str]] = {}
    rule_set = set(valid_rules)
    col_set = set(valid_cols)
    mapping: Dict[str, Any] = raw.get("rule_to_columns", raw if isinstance(raw, dict) else {})

    if not isinstance(mapping, dict):
        return {}

    for rule, columns in mapping.items():
        if rule in rule_set and isinstance(columns, list):
            filtered = sorted({column for column in columns if column in col_set})
            if filtered:
                output[rule] = filtered

    return output


def _build_llm_suggestions(metadata: Dict[str, Any]) -> Tuple[Dict[str, List[str]], Dict[str, str]] | None:
    table_meta = {
        "catalog": metadata["catalog"],
        "schema": metadata["schema"],
        "table_name": metadata["table"]["table_name"],
        "table_description": metadata["table"].get("table_description", ""),
        "columns": [
            {
                "column_name": column["column_name"],
                "data_type": column.get("data_type", ""),
                "description": column.get("description", ""),
            }
            for column in metadata["table"]["columns"]
        ],
    }

    valid_rules = RULE_ORDER[:]
    valid_cols = [column["column_name"] for column in metadata["table"]["columns"]]
    llm = _get_llm_client()

    if llm is None:
        return None

    json_contract = {
        "type": "object",
        "required": ["rule_to_columns"],
        "properties": {
            "rule_to_columns": {
                "type": "object",
                "additionalProperties": {
                    "type": "array",
                    "items": {"type": "string"},
                },
            }
        },
        "additionalProperties": False,
    }

    prompt = f"""
You are a precise data-quality rule suggester.
Only use the provided columns and descriptions.
Never invent column names or rules.
Return ONLY strict JSON with the mapping from rule to array of column names.

{_LLM_RULES_DEF}

Constraints:
- Use ONLY these rule keys exactly: {valid_rules}
- Use ONLY these column names exactly: {valid_cols}
- Base suggestions strictly on column descriptions and business rules. If a rule is not clearly applicable, omit it.
- Return ONLY JSON. No markdown, no commentary.

Output JSON schema (example shape):
{json.dumps(json_contract, indent=2)}

INPUT METADATA:
{json.dumps(table_meta, indent=2)}
"""

    try:
        response = llm.responses.create(
            model=os.getenv("DQ_LLM_MODEL", "gpt-5-mini"),
            input=prompt,
        )
        raw_json = _safe_json_from_text(response.output_text)
        suggestions = _sanitize_llm_mapping(raw_json or {}, valid_rules, valid_cols)
    except Exception:
        suggestions = {}

    if not suggestions:
        return None

    dtype_map = {
        column["column_name"]: column.get("data_type", "")
        for column in metadata["table"]["columns"]
    }
    return suggestions, dtype_map


def _suggestion_cache_key(metadata: Dict[str, Any]) -> str:
    signature = json.dumps(
        {
            "catalog": metadata["catalog"],
            "schema": metadata["schema"],
            "table": metadata["table"],
        },
        sort_keys=True,
    )
    signature_hash = hashlib.md5(signature.encode("utf-8")).hexdigest()
    return f"{full_table_target_str(metadata)}:{signature_hash}"


def _schedule_llm_suggestion_refresh(cache_key: str, metadata: Dict[str, Any]) -> bool:
    if _get_llm_client() is None:
        return False

    with _DQ_SUGGESTION_REFRESH_LOCK:
        if cache_key in _DQ_SUGGESTION_REFRESH_IN_PROGRESS:
            return False
        _DQ_SUGGESTION_REFRESH_IN_PROGRESS.add(cache_key)

    def _worker():
        try:
            llm_output = _build_llm_suggestions(metadata)
            if llm_output is None:
                return

            suggestions, dtype_map = llm_output
            save_dq_cache(
                cache_key,
                {
                    "suggested_columns_by_rule": suggestions,
                    "dtypes_by_column": dtype_map,
                    "suggestion_source": "cached_ai",
                },
            )
        finally:
            with _DQ_SUGGESTION_REFRESH_LOCK:
                _DQ_SUGGESTION_REFRESH_IN_PROGRESS.discard(cache_key)

    threading.Thread(target=_worker, daemon=True).start()
    return True


def build_suggestions(metadata: Dict[str, Any]) -> Tuple[Dict[str, List[str]], Dict[str, str], str]:
    cache_key = _suggestion_cache_key(metadata)
    cached_payload = load_dq_cache(cache_key)
    if cached_payload is not None:
        return (
            cached_payload.get("suggested_columns_by_rule", {}),
            cached_payload.get("dtypes_by_column", {}),
            cached_payload.get("suggestion_source", "cached_ai"),
        )

    static_suggestions, dtype_map = _build_suggestions_static(metadata["table"]["columns"])
    refresh_scheduled = _schedule_llm_suggestion_refresh(cache_key, metadata)
    suggestion_source = "static_pending_ai" if refresh_scheduled else "static"
    return static_suggestions, dtype_map, suggestion_source


def q_ident(name: str) -> str:
    if name is None:
        return ""
    return f"`{name}`".replace(".", "`.`")


def full_table_name(metadata: Dict[str, Any]) -> str:
    return (
        f"{q_ident(metadata['catalog'])}."
        f"{q_ident(metadata['schema'])}."
        f"{q_ident(metadata['table']['table_name'])}"
    )


def full_table_target_str(metadata: Dict[str, Any]) -> str:
    return f"{metadata['catalog']}.{metadata['schema']}.{metadata['table']['table_name']}"


def constraints() -> dict[str, int | str]:
    return {
        "MARKET_NAME_MAX_LEN": int(os.getenv("MARKET_NAME_MAX_LEN", "100")),
        "STAGED_FILE_NAME_MAX_LEN": int(os.getenv("STAGED_FILE_NAME_MAX_LEN", "1024")),
        "FRESHNESS_DAYS": int(os.getenv("FRESHNESS_DAYS", "7")),
        "MONTH_END_ENCODING": os.getenv("MONTH_END_ENCODING", "yyyyMMdd"),
    }


def make_count_sql(metadata: Dict[str, Any], rule: str, column: str, dtype: str) -> str | None:
    table_ref = full_table_name(metadata)
    column_ref = f"`{column}`"
    config = constraints()

    def isnan_expr() -> str:
        return f" OR ISNAN({column_ref})" if (dtype or "").upper() == "DOUBLE" else ""

    if rule == "not_null":
        return f"SELECT COUNT(*) FROM {table_ref} WHERE {column_ref} IS NULL"

    if rule == "no_blank_strings":
        if (dtype or "").upper() != "STRING":
            return None
        return f"SELECT COUNT(*) FROM {table_ref} WHERE {column_ref} IS NOT NULL AND TRIM({column_ref}) = ''"

    if rule == "length_validation":
        if column == "MARKET_NAME":
            return f"SELECT COUNT(*) FROM {table_ref} WHERE LENGTH({column_ref}) > {config['MARKET_NAME_MAX_LEN']}"
        if column == "STAGED_FILE_NAME":
            return f"SELECT COUNT(*) FROM {table_ref} WHERE LENGTH({column_ref}) > {config['STAGED_FILE_NAME_MAX_LEN']}"
        return None

    if rule == "unique_values":
        return (
            f"SELECT COALESCE(SUM(1), 0) FROM ("
            f" SELECT {column_ref}, COUNT(*) AS cnt"
            f" FROM {table_ref}"
            f" WHERE {column_ref} IS NOT NULL"
            f" GROUP BY {column_ref}"
            f" HAVING COUNT(*) > 1"
            f") duplicates"
        )

    if rule == "numeric_range_validation":
        if column == "MARKET_ID":
            return f"SELECT COUNT(*) FROM {table_ref} WHERE {column_ref} < 0"
        if column == "PRODUCT_ID":
            return f"SELECT COUNT(*) FROM {table_ref} WHERE {column_ref} IS NULL OR {column_ref} <= 0"
        if column == "REL_ID":
            return f"SELECT COUNT(*) FROM {table_ref} WHERE {column_ref} < 0"
        if column == "WAC_DOLLARS":
            return f"SELECT COUNT(*) FROM {table_ref} WHERE {column_ref} < 0{isnan_expr()}"
        if column == "AWP":
            return f"SELECT COUNT(*) FROM {table_ref} WHERE {column_ref} < 0{isnan_expr()}"
        if column == "VOLUME_UNITS":
            return f"SELECT COUNT(*) FROM {table_ref} WHERE {column_ref} < 0{isnan_expr()}"
        if column == "PACK_UNITS":
            return f"SELECT COUNT(*) FROM {table_ref} WHERE {column_ref} < 1{isnan_expr()}"
        if column == "EXTENDED_UNITS":
            return f"SELECT COUNT(*) FROM {table_ref} WHERE {column_ref} < 0{isnan_expr()}"
        return None

    if rule == "zero_value_check":
        if column in {"WAC_DOLLARS", "PACK_UNITS"}:
            return f"SELECT COUNT(*) FROM {table_ref} WHERE {column_ref} = 0"
        return None

    if rule == "no_future_dates":
        if (dtype or "").upper() == "DATE":
            return f"SELECT COUNT(*) FROM {table_ref} WHERE {column_ref} > CURRENT_DATE()"
        if column == "MONTH_ENDING_DATE":
            if config["MONTH_END_ENCODING"] == "epoch_ms":
                parsed = f"TO_DATE(TO_TIMESTAMP({column_ref} / 1000))"
            else:
                parsed = f"TO_DATE(CAST({column_ref} AS STRING), 'yyyyMMdd')"
            return f"SELECT COUNT(*) FROM {table_ref} WHERE {parsed} IS NULL OR {parsed} > CURRENT_DATE()"
        return None

    if rule == "freshness_validation":
        if column == "STAGED_FILE_DATE":
            return (
                f"SELECT COUNT(*) FROM {table_ref}"
                f" WHERE {column_ref} < DATEADD(day, -{config['FRESHNESS_DAYS']}, CURRENT_DATE())"
            )
        return None

    return None


def run_count_query(count_sql: str) -> Tuple[int | None, str | None]:
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(count_sql)
                row = cursor.fetchone()
                if row is None:
                    return 0, None
                return int(row[0]) if row[0] is not None else 0, None
    except Exception as exc:
        return None, f"Execution error: {exc}"


def parse_selections_from_payload(payload: Dict[str, Any]) -> List[Tuple[str, str]]:
    selections: List[Tuple[str, str]] = []

    if not payload:
        return selections

    if isinstance(payload.get("selections"), list):
        for item in payload["selections"]:
            rule = (item or {}).get("rule")
            column = (item or {}).get("column")
            if rule and column:
                selections.append((rule, column))

    if isinstance(payload.get("selected_keys"), list):
        for key in payload["selected_keys"]:
            if isinstance(key, str) and key.startswith("sel__"):
                _, rule, column = key.split("__", 2)
                selections.append((rule, column))

    for key, value in payload.items():
        if isinstance(key, str) and key.startswith("sel__") and value:
            _, rule, column = key.split("__", 2)
            selections.append((rule, column))

    seen = set()
    unique = []
    for selection in selections:
        if selection not in seen:
            seen.add(selection)
            unique.append(selection)

    return unique


def build_suggestion_response(catalog: str, schema: str, table: str) -> dict[str, Any]:
    metadata = get_table_metadata(catalog, schema, table)
    suggestions, dtypes, suggestion_source = build_suggestions(metadata)

    return {
        "target": full_table_target_str(metadata),
        "note": "Cached AI suggestions are returned when available; otherwise a static fallback is returned immediately while AI refresh runs in the background.",
        "rules_order": RULE_ORDER,
        "rule_titles": RULE_TITLES,
        "suggested_columns_by_rule": suggestions,
        "dtypes_by_column": dtypes,
        "suggestion_source": suggestion_source,
        "metadata_source": str(DOCUMENTATION_PATH),
    }


def build_run_response(catalog: str, schema: str, table: str, payload: Dict[str, Any]) -> dict[str, Any]:
    metadata = get_table_metadata(catalog, schema, table)
    selections = parse_selections_from_payload(payload)

    if not selections:
        return {
            "target": full_table_target_str(metadata),
            "selections": [],
            "failures_by_column": {},
            "errors": [],
            "summary": {
                "total_checks_requested": 0,
                "total_checks_evaluated": 0,
                "total_columns_with_failures": 0,
                "all_passed": True,
            },
            "message": "No checks were selected.",
            "metadata_source": str(DOCUMENTATION_PATH),
        }

    columns = metadata["table"]["columns"]
    dtype_map: Dict[str, str] = {column["column_name"]: column.get("data_type", "") for column in columns}
    failures_by_col: Dict[str, List[Dict[str, Any]]] = {}
    errors_list: List[Dict[str, Any]] = []

    for rule, column in selections:
        dtype = dtype_map.get(column, "")
        count_sql = make_count_sql(metadata, rule, column, dtype)

        if not count_sql:
            continue

        violations, error = run_count_query(count_sql)
        if error is not None or violations is None:
            errors_list.append(
                {
                    "column": column,
                    "rule": rule,
                    "error": error or "Unknown error",
                }
            )
            continue

        if violations > 0:
            failures_by_col.setdefault(column, []).append(
                {
                    "rule": rule,
                    "title": RULE_TITLES.get(rule, rule),
                    "violations": int(violations),
                    "sql": count_sql,
                }
            )

    evaluated = sum(
        1
        for rule, column in selections
        if make_count_sql(metadata, rule, column, dtype_map.get(column, "")) is not None
    )
    all_passed = evaluated > 0 and not failures_by_col and not errors_list

    response = {
        "target": full_table_target_str(metadata),
        "selections": [{"rule": rule, "column": column} for rule, column in selections],
        "failures_by_column": failures_by_col,
        "errors": errors_list,
        "summary": {
            "total_checks_requested": len(selections),
            "total_checks_evaluated": evaluated,
            "total_columns_with_failures": len(failures_by_col),
            "all_passed": all_passed,
        },
        "metadata_source": str(DOCUMENTATION_PATH),
    }

    if all_passed:
        response["message"] = "All selected checks passed - no violations found."

    return response
