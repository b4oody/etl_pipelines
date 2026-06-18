import pandas as pd
import logging

from etl.transform.schemas import FACT_EXCHANGE_RATE_SCHEMA

logger = logging.getLogger(__name__)

# Колонки, які мають прийти від API НБУ для цього трансформу.
REQUIRED_SOURCE_COLUMNS = ["r030", "rate", "exchangedate"]


def transform_exchange_rates(raw_data):
    """
    Трансформувати сирі дані курсів валют

    Args:
        raw_data: list of dicts від API

    Returns:
        pd.DataFrame: Трансформований та валідований DataFrame
    """
    try:
        # ── Вхідна валідація: ловимо проблеми ДО обробки, зі зрозумілою помилкою ──
        if not isinstance(raw_data, list) or len(raw_data) == 0:
            raise ValueError(
                f"Очікувався непорожній list від API, отримано: "
                f"{type(raw_data).__name__}, len={len(raw_data) if hasattr(raw_data, '__len__') else 'n/a'}"
            )

        logger.info(f"Transforming {len(raw_data)} exchange rates...")

        # Крок 1: Convert to DataFrame
        df = pd.json_normalize(raw_data)
        logger.debug(f"After normalize: {len(df)} rows")

        # Перевірка, що API віддав потрібні колонки (інакше — зрозуміла помилка
        # замість KeyError десь нижче, якщо НБУ змінить схему).
        missing = [c for c in REQUIRED_SOURCE_COLUMNS if c not in df.columns]
        if missing:
            raise ValueError(
                f"У відповіді API бракує колонок {missing}. "
                f"Наявні: {list(df.columns)}. Можливо, змінилась схема API НБУ."
            )

        # Крок 2: Select потрібні колонки
        df = df[REQUIRED_SOURCE_COLUMNS]

        # Крок 3: Rename колонки
        df.columns = [
            "currency_id",
            "rate",
            "rate_date",
        ]

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
        df['rate'] = df['rate'].astype('float64')
        # API НБУ віддає дату як DD.MM.YYYY (напр. "05.06.2026").
        # Явний format обовʼязковий: без нього pd.to_datetime вгадує і плутає
        # день з місяцем там, де обидва <= 12 (05.06 -> помилково 6 травня).
        df['rate_date'] = pd.to_datetime(df['rate_date'], format='%d.%m.%Y')
        # df['special'] = df['special'].astype('int64')

        # ── Вихідна валідація: типи, діапазони, унікальність PK, дати не в майбутньому.
        # lazy=True — зібрати ВСІ помилки одразу, а не падати на першій.
        df = FACT_EXCHANGE_RATE_SCHEMA.validate(df, lazy=True)

        logger.info(f"[OK] Transformed {len(df)} exchange rates successfully.")

        return df

    except Exception as e:
        logger.error(f"[ERROR] Transform failed: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    # Для локального тестування
    from ..extract.fact_exchange_rate import extract_exchange_rates

    raw_data = extract_exchange_rates()
    transformed_df = transform_exchange_rates(raw_data)
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