import json
import os
import threading
from typing import Any

from app.db.redis import get_redis_client

DQ_CACHE_KEY_PREFIX = "dq_suggestions"
DQ_CACHE_TTL_SECONDS = int(os.getenv("DQ_CACHE_TTL_SECONDS", "86400"))

_memory_cache: dict[str, dict[str, Any]] = {}
_memory_cache_lock = threading.Lock()


def _redis_key(cache_key: str) -> str:
    return f"{DQ_CACHE_KEY_PREFIX}:{cache_key}"


def load_dq_cache(cache_key: str) -> dict[str, Any] | None:
    with _memory_cache_lock:
        cached = _memory_cache.get(cache_key)
        if cached is not None:
            return cached

    redis_client = get_redis_client()
    if redis_client is None:
        return None

    try:
        raw = redis_client.get(_redis_key(cache_key))
        if not raw:
            return None

        payload = json.loads(raw)
        with _memory_cache_lock:
            _memory_cache[cache_key] = payload
        return payload
    except Exception:
        return None


def save_dq_cache(cache_key: str, payload: dict[str, Any]) -> bool:
    with _memory_cache_lock:
        _memory_cache[cache_key] = payload

    redis_client = get_redis_client()
    if redis_client is None:
        return False

    try:
        redis_client.setex(_redis_key(cache_key), DQ_CACHE_TTL_SECONDS, json.dumps(payload))
        return True
    except Exception:
        return False
