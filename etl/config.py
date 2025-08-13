import os

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", 5432))
DB_NAME = os.getenv("DB_NAME", "etl_db")
DB_USER = os.getenv("DB_USER", "etl_user")
DB_PASS = os.getenv("DB_PASS", "etl_pass")

FEED_DIR = os.getenv("FEED_DIR", "/data/feed")
INTERVAL_MINUTES = int(os.getenv("INTERVAL_MINUTES", "60"))
