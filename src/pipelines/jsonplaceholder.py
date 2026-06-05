import logging
import sys

from config import DATABASES, LOG_LEVEL, LOG_FILE
from src.connectors.postgresql import PostgreSQLConnector
from src.extract.users import extract_users
from src.extract.posts import extract_posts
from src.extract.comments import extract_comments
from src.transform.users import transform_users
from src.transform.posts import transform_posts
from src.transform.comments import transform_comments
from src.load.users import load_users
from src.load.posts import load_posts
from src.load.comments import load_comments

logging.basicConfig(
    level=LOG_LEVEL,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


def run_jsonplaceholder_etl():
    """Запустити JSONPlaceholder ETL pipeline з PostgreSQL"""

    logger.info("=" * 60)
    logger.info("Starting JSONPlaceholder ETL Pipeline (PostgreSQL)")
    logger.info("=" * 60)

    # Підключатися до БД
    db_config = DATABASES["jsonplaceholder"]
    db_connector = PostgreSQLConnector(
        host=db_config["host"],
        port=db_config["port"],
        database=db_config["database"],
        user=db_config["user"],
        password=db_config["password"],
    )
    db_connector.connect()

    try:
        # ===== USERS =====
        logger.info("\nUSERS PIPELINE")
        logger.info("-" * 60)

        raw_users = extract_users()
        transformed_users = transform_users(raw_users)
        result_users = load_users(transformed_users, db_connector.conn, mode="upsert")

        logger.info(f"Users result: {result_users}\n")

        # ===== POSTS =====
        logger.info("POSTS PIPELINE")
        logger.info("-" * 60)

        raw_posts = extract_posts()
        transformed_posts = transform_posts(raw_posts)
        result_posts = load_posts(transformed_posts, db_connector.conn, mode="upsert")

        logger.info(f"Posts result: {result_posts}\n")

        # ===== COMMENTS =====
        logger.info("COMMENTS PIPELINE")
        logger.info("-" * 60)

        raw_comments = extract_comments()
        transformed_comments = transform_comments(raw_comments)
        result_comments = load_comments(transformed_comments, db_connector.conn, mode="upsert")

        logger.info(f"Comments result: {result_comments}\n")

        # ===== SUMMARY =====
        logger.info("=" * 60)
        logger.info("ETL Pipeline completed successfully!")
        logger.info("=" * 60)
        logger.info(f"Users: {result_users['rows']} rows loaded")
        logger.info(f"Posts: {result_posts['rows']} rows loaded")
        logger.info(f"Comments: {result_comments['rows']} rows loaded")
        logger.info("=" * 60)

        return {
            "status": "success",
            "users": result_users,
            "posts": result_posts,
            "comments": result_comments
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
