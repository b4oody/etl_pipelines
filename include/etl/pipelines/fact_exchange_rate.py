import logging
import sys
from datetime import date, datetime, timedelta

from config import DATABASES, LOG_LEVEL, LOG_FILE
from etl.connectors.postgresql import PostgreSQLConnector
from etl.extract.fact_exchange_rate import extract_exchange_rates
from etl.transform.fact_exchange_rate import transform_exchange_rates
from etl.load.fact_exchange_rate import load_fact_exchange_rates
from etl.transform.dim_currency import transform_dim_currency
from etl.load.dim_currency import load_dim_currency


logging.basicConfig(
    level=LOG_LEVEL,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


def iter_dates(start_date, end_date=None):
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()

    if end_date is None:
        end_date = date.today()
    elif isinstance(end_date, str):
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

    current_date = start_date
    while current_date <= end_date:
        yield current_date
        current_date += timedelta(days=1)


def run_fact_exchange_rate_etl(start_date=None, end_date=None):
    """Запустити ETL pipeline для фактичних курсів валют"""

    logger.info("=" * 60)
    logger.info("Starting Fact Exchange Rates ETL Pipeline (PostgreSQL)")
    logger.info("=" * 60)

    # Підключатися до БД
    db_config = DATABASES["postgres_currency"]
    db_connector = PostgreSQLConnector(
        host=db_config["host"],
        port=db_config["port"],
        database=db_config["database"],
        user=db_config["user"],
        password=db_config["password"],
    )
    db_connector.connect()

    try:
        # ===== fact_exchange_rate =====
        logger.info("\nFACT_EXCHANGE_RATE PIPELINE")
        logger.info("-" * 60)

        if start_date is None:
            raw_fact_exchange_rate = extract_exchange_rates()
            transformed_dim_currency = transform_dim_currency(raw_fact_exchange_rate)
            load_dim_currency(transformed_dim_currency, db_connector.conn, mode="upsert")
            transformed_fact_exchange_rate = transform_exchange_rates(raw_fact_exchange_rate)
            result_fact_exchange_rate = load_fact_exchange_rates(transformed_fact_exchange_rate, db_connector.conn, mode="upsert")
        else:
            total_rows = 0

            for rate_date in iter_dates(start_date, end_date):
                logger.info(f"Processing exchange rates for {rate_date}")

                raw_fact_exchange_rate = extract_exchange_rates(rate_date=rate_date)
                transformed_dim_currency = transform_dim_currency(raw_fact_exchange_rate)
                load_dim_currency(transformed_dim_currency, db_connector.conn, mode="upsert")
                transformed_fact_exchange_rate = transform_exchange_rates(raw_fact_exchange_rate)
                result = load_fact_exchange_rates(transformed_fact_exchange_rate, db_connector.conn, mode="upsert")
                total_rows += result["rows"]

            result_fact_exchange_rate = {
                "status": "success",
                "rows": total_rows,
                "table": "fact_exchange_rate",
            }

        logger.info(f"Fact Exchange Rates result: {result_fact_exchange_rate}\n")


        # ===== SUMMARY =====
        logger.info("=" * 60)
        logger.info("ETL Pipeline completed successfully!")
        logger.info("=" * 60)
        logger.info(f"Fact Exchange Rates: {result_fact_exchange_rate['rows']} rows loaded")
        logger.info("=" * 60)

        return {
            "status": "success",
            "fact_exchange_rate": result_fact_exchange_rate,
        }

    except Exception as e:
        logger.error("\n" + "=" * 60)
        logger.error("ETL Pipeline failed!")
        logger.error("=" * 60)
        logger.error(f"Error: {e}", exc_info=True)
        logger.error("=" * 60)

        return {"status": "failed", "error": str(e)}

    finally:
        db_connector.close()
