import pandas as pd
import logging
from typing import Dict, Any

from etl.loaders.postgresql_loader import PostgreSQLLoader

logger = logging.getLogger(__name__)


def load_comments(df: pd.DataFrame, connection, mode: str = "insert") -> Dict[str, Any]:
    """
    Завантажити коментарі у PostgreSQL

    Args:
        df: DataFrame з трансформованими даними
        connection: PostgreSQL connection
        mode: 'insert', 'upsert', 'replace'

    Returns:
        dict: Результат завантаження
    """
    try:
        if df is None or df.empty:
            logger.warning("[WARN] DataFrame is empty for comments")
            return {"status": "warning", "rows": 0}

        logger.info(f"Loading {len(df)} comments to PostgreSQL...")

        loader = PostgreSQLLoader(connection, "comments", primary_key="comment_id")
        result = loader.load(df, mode=mode)

        logger.info(f"[OK] Loaded {result['rows']} comments")

        return result

    except Exception as e:
        logger.error(f"[ERROR] Load failed: {e}", exc_info=True)
        raise