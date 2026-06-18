import logging
import sys

from config import DATABASES, LOG_LEVEL, LOG_FILE
from etl.load.dim_currency import load_dim_currency
from etl.connectors.postgresql import PostgreSQLConnector
from etl.extract.fact_exchange_rate import extract_exchange_rates
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


def run_dim_currency_etl():
    """Запустити ETL pipeline для dimensionally таблиці валют"""

    logger.info("=" * 60)
    logger.info("Starting Dim Currency ETL Pipeline (PostgreSQL)")
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
        # ===== dim_currency =====
        logger.info("\nDIM_CURRENCY PIPELINE")
        logger.info("-" * 60)

        raw_dim_currency = extract_exchange_rates()
        transformed_dim_currency = transform_dim_currency(raw_dim_currency)
        result_dim_currency = load_dim_currency(transformed_dim_currency, db_connector.conn, mode="upsert")

        logger.info(f"Dim Currency result: {result_dim_currency}\n")


        # ===== SUMMARY =====
        logger.info("=" * 60)
        logger.info("ETL Pipeline completed successfully!")
        logger.info("=" * 60)
        logger.info(f"Dim Currency: {result_dim_currency['rows']} rows loaded")
        logger.info("=" * 60)

        return {
            "status": "success",
            "dim_currency": result_dim_currency,
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
