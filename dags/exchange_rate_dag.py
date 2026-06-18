"""
DAG: щоденне завантаження курсів валют НБУ → PostgreSQL.

Кожен DAG Run обробляє ОДНУ дату (logical_date).
Бізнес-логіка лежить у include/etl/ — тут її лише ВИКЛИКАЄМО, не переписуємо.
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from etl.extract.fact_exchange_rate import extract_exchange_rates
from etl.transform.dim_currency import transform_dim_currency
from etl.load.dim_currency import load_dim_currency
from etl.transform.fact_exchange_rate import transform_exchange_rates
from etl.load.fact_exchange_rate import load_fact_exchange_rates    

# ── Імпорти твого ETL-коду з include/etl (легкі імпорти — ОK на верхньому рівні) ──
# TODO 1: імпортуй 5 функцій:
#   - extract_exchange_rates       з include.etl.extract.fact_exchange_rate
#   - transform_dim_currency       з include.etl.transform.dim_currency
#   - load_dim_currency            з include.etl.load.dim_currency
#   - transform_exchange_rates     з include.etl.transform.fact_exchange_rate
#   - load_fact_exchange_rates     з include.etl.load.fact_exchange_rate


# default_args — параметри, СПІЛЬНІ для всіх задач DAG.
default_args = {
    "owner": "data-team",
    # TODO 2: додай retries (напр. 3) і retry_delay (pendulum.duration(minutes=5)),
    #         за бажанням execution_timeout.
}


@dag(
    dag_id="exchange_rate_etl",
    description="Щоденні курси валют НБУ → PostgreSQL",
    # TODO 3: для ПЕРШОГО тесту лиши schedule=None (тільки ручний Trigger).
    #         Пізніше зміниш на cron, напр. "0 9 * * *".
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Kyiv"),
    catchup=False,            # не доганяти історію автоматично
    max_active_runs=1,        # не більше 1 запуску одночасно
    default_args=default_args,
    tags=["test1", "currency", "nbu", "etl"],
)
def exchange_rate_etl():

    @task
    def extract(**context) -> list[dict]:
        """Витяг курсів за дату цього DAG Run."""
        extract_date = context["logical_date"].date()  # дата запуску DAG Runs
        # TODO 4: дістань дату запуску з context["logical_date"].date()
        #         і поверни extract_exchange_rates(rate_date=<ця дата>).
        return extract_exchange_rates(rate_date=extract_date)

    @task
    def load_currencies(raw: list[dict]) -> dict:
        """Оновити довідник валют dim_currency (upsert). Має йти ПЕРШИМ (FK)."""
        df = transform_dim_currency(raw)
        conn = PostgresHook(postgres_conn_id="postgres_currency").get_conn()
        try:
            result = load_dim_currency(df, conn, mode="upsert")
            conn.commit()
            return result
        finally:
            conn.close()
        # TODO 5:
        #   1) df = transform_dim_currency(raw)
        #   2) conn = PostgresHook(postgres_conn_id="postgres_currency").get_conn()
        #   3) у try/finally: load_dim_currency(df, conn, mode="upsert"); conn.commit()
        #      finally: conn.close()
        #   4) поверни результат load_dim_currency(...)
        ...

    @task
    def load_rates(raw: list[dict]) -> dict:
        """Трансформувати й завантажити факти курсів fact_exchange_rate (upsert)."""
        # TODO 6: те саме, що load_currencies, але через
        #         transform_exchange_rates(...) і load_fact_exchange_rates(...).
        ...
        df = transform_exchange_rates(raw)
        conn = PostgresHook(postgres_conn_id="postgres_currency").get_conn()
        try:
            result = load_fact_exchange_rates(df, conn, mode="upsert")
            conn.commit()
            return result
        finally:
            conn.close()

    # ── Граф залежностей ──
    # TODO 7: побудуй граф:
    #   raw = extract()
    #   dim_done = load_currencies(raw)
    #   fact_done = load_rates(raw)
    #   далі ЯВНО: спершу довідник, потім факти  ->  dim_done >> fact_done
    ...
    raw = extract()
    dim_done = load_currencies(raw)
    fact_done = load_rates(raw)
    dim_done >> fact_done


# TODO 8: ОБОВ'ЯЗКОВО виклич DAG-функцію внизу, інакше DAG не зареєструється:
# exchange_rate_etl()
exchange_rate_etl()