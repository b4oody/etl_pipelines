import pandas as pd
import logging
from typing import Dict, Any

from etl.loaders.postgresql_loader import PostgreSQLLoader

logger = logging.getLogger(__name__)


def load_posts(df: pd.DataFrame, connection, mode: str = "insert") -> Dict[str, Any]:
    """
    Завантажити пості у PostgreSQL

    Args:
        df: DataFrame з трансформованими даними
        connection: PostgreSQL connection
        mode: 'insert', 'upsert', 'replace'

    Returns:
        dict: Результат завантаження
    """
    try:
        if df is None or df.empty:
            logger.warning("[WARN] DataFrame is empty for posts")
            return {"status": "warning", "rows": 0}

        logger.info(f"Loading {len(df)} posts to PostgreSQL...")

        loader = PostgreSQLLoader(connection, "posts", primary_key="post_id")
        result = loader.load(df, mode=mode)

        logger.info(f"[OK] Loaded {result['rows']} posts")

        return result

    except Exception as e:
        logger.error(f"[ERROR] Load failed: {e}", exc_info=True)
        raise