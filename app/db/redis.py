import os

import redis

from app.core.config import REDIS_HOST, REDIS_PASSWORD, REDIS_PORT, REDIS_SSL

REDIS_CONNECT_TIMEOUT_SECONDS = float(os.getenv("REDIS_CONNECT_TIMEOUT_SECONDS", "1.5"))
REDIS_SOCKET_TIMEOUT_SECONDS = float(os.getenv("REDIS_SOCKET_TIMEOUT_SECONDS", "1.5"))


def _build_redis_client():
    if not REDIS_HOST or not REDIS_PORT:
        return None

    try:
        connection_options = {
            "host": REDIS_HOST,
            "port": int(REDIS_PORT),
            "password": REDIS_PASSWORD,
            "decode_responses": True,
            "socket_connect_timeout": REDIS_CONNECT_TIMEOUT_SECONDS,
            "socket_timeout": REDIS_SOCKET_TIMEOUT_SECONDS,
            "retry_on_timeout": False,
        }

        if REDIS_SSL:
            connection_options["ssl"] = True

        return redis.Redis(**connection_options)
    except Exception:
        return None


redis_client = _build_redis_client()


def get_redis_client():
    return redis_client


def redis_enabled() -> bool:
    return redis_client is not None
