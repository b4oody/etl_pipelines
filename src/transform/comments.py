import pandas as pd
import logging

logger = logging.getLogger(__name__)

def transform_comments(raw_data):
    """
    Трансформувати сирі дані коментарів

    Args:
        raw_data: list of dicts від API

    Returns:
        pd.DataFrame: Трансформований та валідований DataFrame
    """
    try:
        logger.info(f"Transforming {len(raw_data)} comments...")

        # Крок 1: Convert to DataFrame
        df = pd.json_normalize(raw_data)
        logger.debug(f"After normalize: {len(df)} rows")

        # Крок 2: Select потрібні колонки
        df = df[[
            "id",
            "postId",
            "name",
            "email",
            "body"
        ]]

        # Крок 3: Rename колонки
        df.columns = [
            "comment_id",
            "post_id",
            "name",
            "email",
            "body"
        ]

        # Крок 4: Очищення даних
        logger.debug("Cleaning data...")

        initial_count = len(df)

        # Видалити NULLs у критичних полях
        df = df.dropna(subset=['comment_id', 'post_id'])
        removed_nulls = initial_count - len(df)
        if removed_nulls > 0:
            logger.debug(f"Removed {removed_nulls} rows with NULLs")

        # Видалити дублікати за comment_id
        initial_count = len(df)
        df = df.drop_duplicates(subset=['comment_id'], keep='first')
        removed_dupes = initial_count - len(df)
        if removed_dupes > 0:
            logger.debug(f"Removed {removed_dupes} duplicate rows")

        # Крок 5: Валідація даних
        logger.debug("Validating data...")

        # comment_id мав бути > 0
        initial_count = len(df)
        df = df[df['comment_id'] > 0]
        removed_invalid = initial_count - len(df)
        if removed_invalid > 0:
            logger.debug(f"Removed {removed_invalid} rows with invalid comment_id")

        # post_id мав бути > 0
        initial_count = len(df)
        df = df[df['post_id'] > 0]
        removed_invalid = initial_count - len(df)
        if removed_invalid > 0:
            logger.debug(f"Removed {removed_invalid} rows with invalid post_id")

        # email мав містити @
        initial_count = len(df)
        df = df[df['email'].str.contains('@', na=False)]
        removed_invalid = initial_count - len(df)
        if removed_invalid > 0:
            logger.debug(f"Removed {removed_invalid} rows with invalid email")

        # body не може бути порожнім
        initial_count = len(df)
        df = df[df['body'].str.len() > 0]
        removed_invalid = initial_count - len(df)
        if removed_invalid > 0:
            logger.debug(f"Removed {removed_invalid} rows with empty body")

        # Крок 6: Type casting
        df['comment_id'] = df['comment_id'].astype('int64')
        df['post_id'] = df['post_id'].astype('int64')
        df['name'] = df['name'].astype('string')
        df['email'] = df['email'].astype('string').str.lower()
        df['body'] = df['body'].astype('string')

        logger.info(f"[OK] Transformed {len(df)} comments")

        return df

    except Exception as e:
        logger.error(f"[ERROR] Transform failed: {e}", exc_info=True)
        raise