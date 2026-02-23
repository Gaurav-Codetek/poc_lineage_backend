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
from app.db.redis import redis_client

CACHE_KEY = "table_lineage_cache"


def save_cache(upstream, downstream, counts):
    payload = {
        "upstream": upstream,
        "downstream": downstream,
        "counts": counts
    }
    redis_client.set(CACHE_KEY, json.dumps(payload))


def load_cache():
    data = redis_client.get(CACHE_KEY)
    if not data:
        return None
    return json.loads(data)


def clear_cache():
    redis_client.delete(CACHE_KEY)