import redis
from app.utils.config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD, REDIS_SSL

if REDIS_SSL:
    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=int(REDIS_PORT),
        password=REDIS_PASSWORD,
        ssl=True,
        decode_responses=True
    )
else:
    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=int(REDIS_PORT),
        password=REDIS_PASSWORD,
        decode_responses=True
    )