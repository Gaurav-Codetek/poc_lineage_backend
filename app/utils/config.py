import os
from dotenv import load_dotenv

load_dotenv()

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
HTTP_PATH = os.getenv("HTTP_PATH")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN") 

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASS = os.getenv("NEO4J_PASS")

REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = os.getenv("REDIS_PORT")
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
REDIS_SSL = os.getenv("REDIS_SSL", "true").lower() == "true"

NEO4J_URI_2 = os.getenv("NEO4J_URI_2")
NEO4J_USER_2 = os.getenv("NEO4J_USER_2")
NEO4J_PASS_2 = os.getenv("NEO4J_PASS_2")

NEO4J_CONFIG = {
    "uri": NEO4J_URI_2,
    "user": NEO4J_USER_2,
    "password": NEO4J_PASS_2
}

DATABRICKS_CONFIG = {
    "server_hostname": DATABRICKS_HOST,
    "http_path": HTTP_PATH,
    "access_token": ACCESS_TOKEN
}


CATALOG = "intelliegencedatacatalog"