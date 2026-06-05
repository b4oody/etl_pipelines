import psycopg2
from psycopg2 import sql
import logging
import os
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class PostgreSQLConnector:
    """
    PostgreSQL connection manager
    Надає простой інтерфейс до БД
    """

    def __init__(self, host: str, port: int, database: str, user: str, password: str):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.conn = None

    def connect(self):
        """Створити коннекцію до БД"""
        try:
            # Встановлювати PGPASSWORD через env variable для уникнення encoding проблем
            os.environ['PGPASSWORD'] = self.password

            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                client_encoding='utf8'
            )
            logger.info(f"Connected to PostgreSQL at {self.host}:{self.port}/{self.database}")
            return self.conn
        except psycopg2.Error as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise

    @contextmanager
    def get_connection(self):
        """Context manager для коннекції"""
        if self.conn is None:
            self.connect()

        try:
            yield self.conn
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error in transaction: {e}")
            raise
        finally:
            pass

    def close(self):
        """Закрити коннекцію"""
        if self.conn:
            self.conn.close()
            logger.info("PostgreSQL connection closed")

    def execute_query(self, query: str, params: tuple = None) -> list:
        """Виконати SELECT запит"""
        cursor = self.conn.cursor()
        try:
            cursor.execute(query, params or ())
            return cursor.fetchall()
        except psycopg2.Error as e:
            logger.error(f"Query execution failed: {e}")
            raise
        finally:
            cursor.close()

    def execute_update(self, query: str, params: tuple = None) -> int:
        """Виконати INSERT/UPDATE/DELETE запит"""
        cursor = self.conn.cursor()
        try:
            cursor.execute(query, params or ())
            self.conn.commit()
            return cursor.rowcount
        except psycopg2.Error as e:
            self.conn.rollback()
            logger.error(f"Update execution failed: {e}")
            raise
        finally:
            cursor.close()

    def execute_batch(self, query: str, data: list) -> int:
        """Виконати batch INSERT"""
        cursor = self.conn.cursor()
        try:
            cursor.executemany(query, data)
            self.conn.commit()
            rows_affected = cursor.rowcount
            logger.debug(f"Batch insert: {rows_affected} rows")
            return rows_affected
        except psycopg2.Error as e:
            self.conn.rollback()
            logger.error(f"Batch execution failed: {e}")
            raise
        finally:
            cursor.close()
