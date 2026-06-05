import pandas as pd
import logging

logger = logging.getLogger(__name__)

def transform_dim_currency(raw_data):
    """
    Трансформувати сирі дані курсів валют

    Args:
        raw_data: list of dicts від API

    Returns:
        pd.DataFrame: Трансформований та валідований DataFrame
    """
    try:
        logger.info(f"Transforming {len(raw_data)} exchange rates...")

        # Крок 1: Convert to DataFrame
        df = pd.json_normalize(raw_data)
        logger.debug(f"After normalize: {len(df)} rows")

        # Крок 2: Select потрібні колонки
        df = df[[
            "r030",
            "cc",
            "txt",
        ]]

        # Крок 3: Rename колонки
        df.columns = [
            "currency_id",
            "currency_code",
            "currency_name",
        ]

        # Видалити дублікати за currency_id
        df = df.drop_duplicates(subset=['currency_id'], keep='first')

        # Крок 4: Очищення даних
        logger.debug("Cleaning data...")

        initial_count = len(df)

    #     # Видалити NULLs у критичних полях
    #     df = df.dropna(subset=['comment_id', 'post_id'])
    #     removed_nulls = initial_count - len(df)
    #     if removed_nulls > 0:
    #         logger.debug(f"Removed {removed_nulls} rows with NULLs")

    #     # Видалити дублікати за comment_id
    #     initial_count = len(df)
    #     df = df.drop_duplicates(subset=['comment_id'], keep='first')
    #     removed_dupes = initial_count - len(df)
    #     if removed_dupes > 0:
    #         logger.debug(f"Removed {removed_dupes} duplicate rows")

    #     # Крок 5: Валідація даних
    #     logger.debug("Validating data...")

    #     # comment_id мав бути > 0
    #     initial_count = len(df)
    #     df = df[df['comment_id'] > 0]
    #     removed_invalid = initial_count - len(df)
    #     if removed_invalid > 0:
    #         logger.debug(f"Removed {removed_invalid} rows with invalid comment_id")

    #     # post_id мав бути > 0
    #     initial_count = len(df)
    #     df = df[df['post_id'] > 0]
    #     removed_invalid = initial_count - len(df)
    #     if removed_invalid > 0:
    #         logger.debug(f"Removed {removed_invalid} rows with invalid post_id")

    #     # email мав містити @
    #     initial_count = len(df)
    #     df = df[df['email'].str.contains('@', na=False)]
    #     removed_invalid = initial_count - len(df)
    #     if removed_invalid > 0:
    #         logger.debug(f"Removed {removed_invalid} rows with invalid email")

    #     # body не може бути порожнім
    #     initial_count = len(df)
    #     df = df[df['body'].str.len() > 0]
    #     removed_invalid = initial_count - len(df)
    #     if removed_invalid > 0:
    #         logger.debug(f"Removed {removed_invalid} rows with empty body")

        # Крок 6: Type casting
        df['currency_id'] = df['currency_id'].astype('int64')
        df['currency_code'] = df['currency_code'].astype('str')
        df['currency_name'] = df['currency_name'].astype('str')
        # df['special'] = df['special'].astype('int64')

        logger.info(f"[OK] Transformed {len(df)} exchange dim_currency successfully.")

        return df

    except Exception as e:
        logger.error(f"[ERROR] Transform failed: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    # Для локального тестування
    from ..extract.fact_exchange_rate import extract_exchange_rates

    raw_data = extract_exchange_rates()
    transformed_df = transform_dim_currency(raw_data)
    print(transformed_df.head())
    print("-" * 150)
    print(transformed_df.sample(20))
    print("-" * 150)
    print(transformed_df.shape)
    print("-" * 150)
    print(transformed_df.info())
    print("-" * 150)
    print(transformed_df.dtypes)
    print("-" * 150)
    print(transformed_df.describe())