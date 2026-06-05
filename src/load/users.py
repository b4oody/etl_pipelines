import pandas as pd
import logging
from typing import Dict, Any

from src.loaders.postgresql_loader import PostgreSQLLoader

logger = logging.getLogger(__name__)


def load_users(df: pd.DataFrame, connection, mode: str = "upsert") -> Dict[str, Any]:
    """
    Завантажити користувачів у PostgreSQL

    Args:
        df: DataFrame з трансформованими даними
        connection: PostgreSQL connection
        mode: 'insert', 'upsert', 'replace'

    Returns:
        dict: Результат завантаження
    """
    try:
        if df is None or df.empty:
            logger.warning("[WARN] DataFrame is empty for users")
            return {"status": "warning", "rows": 0}

        logger.info(f"Loading {len(df)} users to PostgreSQL...")

        loader = PostgreSQLLoader(connection, "users", primary_key="user_id")
        result = loader.load(df, mode=mode)

        logger.info(f"[OK] Loaded {result['rows']} users")

        return result

    except Exception as e:
        logger.error(f"[ERROR] Load failed: {e}", exc_info=True)
        raise