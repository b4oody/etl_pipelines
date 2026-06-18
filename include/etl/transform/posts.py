import pandas as pd
import logging

logger = logging.getLogger(__name__)

def transform_posts(raw_data):
    """
    Трансформувати сирі дані постів

    Args:
        raw_data: list of dicts від API

    Returns:
        pd.DataFrame: Трансформований та валідований DataFrame
    """
    try:
        logger.info(f"Transforming {len(raw_data)} posts...")

        # Крок 1: Convert to DataFrame
        df = pd.json_normalize(raw_data)
        logger.debug(f"After normalize: {len(df)} rows")

        # Крок 2: Select потрібні колонки
        df = df[[
            "id",
            "userId",
            "title",
            "body"
        ]]

        # Крок 3: Rename колонки
        df.columns = [
            "post_id",
            "user_id",
            "title",
            "body"
        ]

        # Крок 4: Очищення даних
        logger.debug("Cleaning data...")

        initial_count = len(df)

        # Видалити NULLs у критичних полях
        df = df.dropna(subset=['post_id', 'user_id'])
        removed_nulls = initial_count - len(df)
        if removed_nulls > 0:
            logger.debug(f"Removed {removed_nulls} rows with NULLs")

        # Видалити дублікати за post_id
        initial_count = len(df)
        df = df.drop_duplicates(subset=['post_id'], keep='first')
        removed_dupes = initial_count - len(df)
        if removed_dupes > 0:
            logger.debug(f"Removed {removed_dupes} duplicate rows")

        # Крок 5: Валідація даних
        logger.debug("Validating data...")

        # post_id мав бути > 0
        initial_count = len(df)
        df = df[df['post_id'] > 0]
        removed_invalid = initial_count - len(df)
        if removed_invalid > 0:
            logger.debug(f"Removed {removed_invalid} rows with invalid post_id")

        # user_id мав бути > 0
        initial_count = len(df)
        df = df[df['user_id'] > 0]
        removed_invalid = initial_count - len(df)
        if removed_invalid > 0:
            logger.debug(f"Removed {removed_invalid} rows with invalid user_id")

        # title та body не можуть бути порожніми
        initial_count = len(df)
        df = df[(df['title'].str.len() > 0) & (df['body'].str.len() > 0)]
        removed_invalid = initial_count - len(df)
        if removed_invalid > 0:
            logger.debug(f"Removed {removed_invalid} rows with empty title/body")

        # Крок 6: Type casting
        df['post_id'] = df['post_id'].astype('int64')
        df['user_id'] = df['user_id'].astype('int64')
        df['title'] = df['title'].astype('string')
        df['body'] = df['body'].astype('string')

        logger.info(f"[OK] Transformed {len(df)} posts")

        return df

    except Exception as e:
        logger.error(f"[ERROR] Transform failed: {e}", exc_info=True)
        raise