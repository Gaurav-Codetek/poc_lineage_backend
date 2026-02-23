from app.db.redis import redis_client
from dotenv import load_dotenv
load_dotenv()

redis_client.set("test", "hello")
print(redis_client.get("test"))