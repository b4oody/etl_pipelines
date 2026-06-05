import os
from dotenv import load_dotenv

load_dotenv()
# Named database configurations.
# Кожен ключ - окремий сервіс/джерело даних, навіть якщо всі вони PostgreSQL.
DATABASES = {
    "jsonplaceholder": {
        "type": os.getenv("JSONPLACEHOLDER_DB_TYPE", os.getenv("DB_TYPE", "postgresql")),
        "host": os.getenv("JSONPLACEHOLDER_DB_HOST", os.getenv("DB_HOST", "localhost")),
        "port": int(os.getenv("JSONPLACEHOLDER_DB_PORT", os.getenv("DB_PORT", 5432))),
        "database": os.getenv("JSONPLACEHOLDER_DB_NAME", os.getenv("DB_NAME", "etl_db")),
        "user": os.getenv("JSONPLACEHOLDER_DB_USER", os.getenv("DB_USER", "etl_user")),
        "password": os.getenv("JSONPLACEHOLDER_DB_PASSWORD", os.getenv("DB_PASSWORD", "etl_password")),
    },

    "postgres_currency": {
        "type": os.getenv("POSTGRES_CURRENCY_DB_TYPE", os.getenv("DB_TYPE", "postgresql")),
        "host": os.getenv("POSTGRES_CURRENCY_DB_HOST", os.getenv("DB_HOST", "localhost")),
        "port": int(os.getenv("POSTGRES_CURRENCY_DB_PORT", os.getenv("DB_PORT", 5432))),
        "database": os.getenv("POSTGRES_CURRENCY_DB_NAME", os.getenv("DB_NAME", "etl_db")),
        "user": os.getenv("POSTGRES_CURRENCY_DB_USER", os.getenv("DB_USER", "etl_user")),
        "password": os.getenv("POSTGRES_CURRENCY_DB_PASSWORD", os.getenv("DB_PASSWORD", "etl_password")),
    },

    # Example for future database/service:
    # "abstract_service": {
    #     "type": "postgresql",
    #     "host": os.getenv("ABSTRACT_DB_HOST", "localhost"),
    #     "port": int(os.getenv("ABSTRACT_DB_PORT", 5433)),
    #     "database": os.getenv("ABSTRACT_DB_NAME", "abstract_db"),
    #     "user": os.getenv("ABSTRACT_DB_USER", "etl_user"),
    #     "password": os.getenv("ABSTRACT_DB_PASSWORD", "etl_password"),
    # },
}

# Backward-compatible aliases for old code.
DB_CONFIG = DATABASES["jsonplaceholder"]
DB_TYPE = DATABASES["jsonplaceholder"]["type"]

# Batch size для завантаження
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 100))

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE = os.getenv("LOG_FILE", "etl.log")
