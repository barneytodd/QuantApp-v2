import os
from dotenv import load_dotenv

load_dotenv()

# Environment
APP_ENV = os.getenv("APP_ENV", "local")

# Database
DB_ENGINE = os.getenv("DB_ENGINE", "mssql")

DB_USER = os.getenv("DB_LOCAL_USER") if APP_ENV == "local" else os.getenv("DB_DOCKER_USER")
DB_PASSWORD = os.getenv("DB_LOCAL_PASSWORD") if APP_ENV == "local" else os.getenv("DB_DOCKER_PASSWORD")
DB_HOST = os.getenv("DB_LOCAL_HOST")
DB_PORT = os.getenv("DB_PORT", "1433")
DB_NAME = os.getenv("DB_NAME")
DB_INSTANCE = os.getenv("DB_INSTANCE", "SQLEXPRESS")

# Pool sizing
DB_POOL_MIN = int(os.getenv("DB_POOL_MIN", 1))
DB_POOL_MAX = int(os.getenv("DB_POOL_MAX", 10))