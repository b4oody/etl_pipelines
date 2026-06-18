import pandas as pd
import logging

logger = logging.getLogger(__name__)

def transform_users(raw_data):
    """
    Трансформувати сирі дані користувачів

    Args:
        raw_data: list of dicts від API

    Returns:
        pd.DataFrame: Трансформований та валідований DataFrame
    """
    try:
        logger.info(f"Transforming {len(raw_data)} users...")

        # Крок 1: Convert to DataFrame
        df = pd.json_normalize(raw_data)
        logger.debug(f"After normalize: {len(df)} rows")

        # Крок 2: Select потрібні колонки
        df = df[[
            "id",
            "name",
            "username",
            "email",
            "address.city",
            "company.name"
        ]]

        # Крок 3: Rename колонки
        df.columns = [
            "user_id",
            "name",
            "username",
            "email",
            "city",
            "company_name"
        ]

        # Крок 4: Очищення даних
        logger.debug("Cleaning data...")

        initial_count = len(df)

        # Видалити NULLs у критичних полях
        df = df.dropna(subset=['user_id', 'email'])
        removed_nulls = initial_count - len(df)
        if removed_nulls > 0:
            logger.debug(f"Removed {removed_nulls} rows with NULLs")

        # Видалити дублікати за user_id
        initial_count = len(df)
        df = df.drop_duplicates(subset=['user_id'], keep='first')
        removed_dupes = initial_count - len(df)
        if removed_dupes > 0:
            logger.debug(f"Removed {removed_dupes} duplicate rows")

        # Крок 5: Валідація даних
        logger.debug("Validating data...")

        # user_id мав бути > 0
        initial_count = len(df)
        df = df[df['user_id'] > 0]
        removed_invalid = initial_count - len(df)
        if removed_invalid > 0:
            logger.debug(f"Removed {removed_invalid} rows with invalid user_id")

        # email мав містити @
        initial_count = len(df)
        df = df[df['email'].str.contains('@', na=False)]
        removed_invalid = initial_count - len(df)
        if removed_invalid > 0:
            logger.debug(f"Removed {removed_invalid} rows with invalid email")

        # name не може бути порожнім
        initial_count = len(df)
        df = df[df['name'].str.len() > 0]
        removed_invalid = initial_count - len(df)
        if removed_invalid > 0:
            logger.debug(f"Removed {removed_invalid} rows with empty name")

        # Крок 6: Type casting
        df['user_id'] = df['user_id'].astype('int64')
        df['name'] = df['name'].astype('string')
        df['username'] = df['username'].astype('string')
        df['email'] = df['email'].astype('string').str.lower()
        df['city'] = df['city'].astype('string')
        df['company_name'] = df['company_name'].astype('string')

        logger.info(f"[OK] Transformed {len(df)} users")

        return df

    except Exception as e:
        logger.error(f"[ERROR] Transform failed: {e}", exc_info=True)
        raise