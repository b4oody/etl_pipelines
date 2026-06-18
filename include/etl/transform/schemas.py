"""
Pandera-схеми для валідації вихідних DataFrame перед завантаженням у БД.

Навіщо схеми окремим модулем:
- одне місце правди про структуру даних (типи, діапазони, nullable, унікальність);
- transform-функції лишаються короткими — лише .validate(df) у кінці;
- зрозумілі помилки замість мовчазного запису сміття в БД.
"""
from __future__ import annotations

import pandas as pd
import pandera.pandas as pa
from pandera.pandas import Column, DataFrameSchema, Check


# Схема факту курсів: те, що йде у fact_exchange_rate.
# coerce=False — НЕ підганяємо типи мовчки; транс-функція вже зробила каст,
# схема лише перевіряє, що він коректний.
FACT_EXCHANGE_RATE_SCHEMA = DataFrameSchema(
    {
        "currency_id": Column(
            "int64",
            nullable=False,
            checks=Check.gt(0, error="currency_id має бути > 0"),
        ),
        "rate": Column(
            "float64",
            nullable=False,
            checks=Check.gt(0, error="rate має бути > 0"),
        ),
        "rate_date": Column(
            "datetime64[ns]",
            nullable=False,
            # дата не може бути в майбутньому (захист від плутанини день/місяць)
            checks=Check(
                lambda s: s <= pd.Timestamp.now().normalize(),
                error="rate_date у майбутньому — ймовірно зіпсована дата",
            ),
        ),
    },
    # на рівні всього DataFrame: пара (rate_date, currency_id) унікальна — це PK
    unique=["rate_date", "currency_id"],
    strict=True,   # зайвих колонок бути не повинно
    coerce=False,
)


# Схема довідника валют: dim_currency.
DIM_CURRENCY_SCHEMA = DataFrameSchema(
    {
        "currency_id": Column(
            "int64",
            nullable=False,
            unique=True,
            checks=Check.gt(0, error="currency_id має бути > 0"),
        ),
        "currency_code": Column(
            "object",
            nullable=False,
            checks=Check.str_length(min_value=1, error="currency_code порожній"),
        ),
        "currency_name": Column(
            "object",
            nullable=False,
            checks=Check.str_length(min_value=1, error="currency_name порожній"),
        ),
    },
    strict=True,
    coerce=False,
)
