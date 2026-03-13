# import json
# from app.db.redis import redis_client

# CACHE_KEY = "table_lineage_cache"


# def save_table_cache(data: dict):
#     redis_client.set(CACHE_KEY, json.dumps(data))


# def load_table_cache():
#     data = redis_client.get(CACHE_KEY)
#     if data:
#         return json.loads(data)
#     return None

import json

from app.db.redis import get_redis_client

CACHE_KEY = "table_lineage_cache"


def save_cache(upstream, downstream, counts):
    redis_client = get_redis_client()
    if redis_client is None:
        return False

    payload = {
        "upstream": upstream,
        "downstream": downstream,
        "counts": counts
    }

    try:
        redis_client.set(CACHE_KEY, json.dumps(payload))
        return True
    except Exception:
        return False


def load_cache():
    redis_client = get_redis_client()
    if redis_client is None:
        return None

    try:
        data = redis_client.get(CACHE_KEY)
        if not data:
            return None
        return json.loads(data)
    except Exception:
        return None


def clear_cache():
    redis_client = get_redis_client()
    if redis_client is None:
        return False

    try:
        redis_client.delete(CACHE_KEY)
        return True
    except Exception:
        return False
