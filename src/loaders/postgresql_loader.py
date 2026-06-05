import pandas as pd
import logging
from typing import Dict, Any, List, Union
from psycopg2 import sql

from src.loaders.base_loader import BaseLoader

logger = logging.getLogger(__name__)


class PostgreSQLLoader(BaseLoader):
    """
    Loader для PostgreSQL
    Поддерживает insert, upsert, replace режими
    """

    def __init__(
        self,
        connection,
        table_name: str,
        primary_key: Union[str, List[str]] = "id",
        batch_size: int = 100,
    ):
        super().__init__(table_name)
        self.connection = connection
        self.primary_key = primary_key
        self.batch_size = batch_size

    def load(self, df: pd.DataFrame, mode: str = "upsert") -> Dict[str, Any]:
        """
        Завантажити DataFrame у PostgreSQL
        mode: 'insert', 'upsert', 'replace'
        """
        try:
            if df is None or df.empty:
                logger.warning(f"DataFrame is empty for {self.table_name}")
                return {"status": "warning", "rows": 0}

            logger.info(f"Loading {len(df)} rows to {self.table_name} (mode: {mode})")

            # Розділити на батчі
            batches = [df.iloc[i : i + self.batch_size] for i in range(0, len(df), self.batch_size)]

            total_rows = 0

            for batch_num, batch in enumerate(batches):
                rows_loaded = self._load_batch(batch, mode)
                total_rows += rows_loaded
                logger.debug(f"Batch {batch_num + 1}/{len(batches)}: {rows_loaded} rows loaded")

            logger.info(f"[OK] Loaded {total_rows} rows to {self.table_name}")

            return {"status": "success", "rows": total_rows, "table": self.table_name}

        except Exception as e:
            logger.error(f"[ERROR] Load failed: {e}", exc_info=True)
            raise

    def _load_batch(self, batch: pd.DataFrame, mode: str) -> int:
        """Завантажити один батч"""

        if mode == "insert":
            return self._insert_batch(batch)
        elif mode == "upsert":
            return self._upsert_batch(batch)
        elif mode == "replace":
            return self._replace_batch(batch)
        else:
            raise ValueError(f"Unknown mode: {mode}")

    def _insert_batch(self, batch: pd.DataFrame) -> int:
        """INSERT режим"""
        cursor = self.connection.cursor()

        try:
            # Будуємо INSERT запит
            columns = batch.columns.tolist()
            placeholders = ",".join(["%s"] * len(columns))
            column_names = ",".join(columns)

            query = sql.SQL(f"INSERT INTO {self.table_name} ({column_names}) VALUES ({placeholders})")

            # Конвертуємо DataFrame до tuple list
            data = [tuple(row) for row in batch.values]

            cursor.executemany(query, data)
            self.connection.commit()

            return cursor.rowcount

        except Exception as e:
            self.connection.rollback()
            logger.error(f"Insert failed: {e}")
            raise
        finally:
            cursor.close()

    def _upsert_batch(self, batch: pd.DataFrame) -> int:
        """UPSERT режим (INSERT or UPDATE)"""
        cursor = self.connection.cursor()

        try:
            columns = batch.columns.tolist()
            column_names = ",".join(columns)

            primary_keys = self.primary_key if isinstance(self.primary_key, list) else [self.primary_key]

            # Будуємо UPSERT (ON CONFLICT) запит
            conflict_clause = ",".join(primary_keys)
            set_clause = ",".join([f"{col}=EXCLUDED.{col}" for col in columns if col not in primary_keys])
            placeholders = ",".join(["%s"] * len(columns))

            query = f"""
                INSERT INTO {self.table_name} ({column_names})
                VALUES ({placeholders})
                ON CONFLICT ({conflict_clause}) DO UPDATE SET {set_clause}
            """

            # Конвертуємо DataFrame до tuple list
            data = [tuple(row) for row in batch.values]

            cursor.executemany(query, data)
            self.connection.commit()

            return len(data)

        except Exception as e:
            self.connection.rollback()
            logger.error(f"Upsert failed: {e}")
            raise
        finally:
            cursor.close()

    def _replace_batch(self, batch: pd.DataFrame) -> int:
        """REPLACE режим (DELETE всю таблицю, потім INSERT)"""
        cursor = self.connection.cursor()

        try:
            # Це перший батч?
            if batch.index[0] == 0:
                # Видалити всю таблицю
                query = f"TRUNCATE TABLE {self.table_name} CASCADE"
                cursor.execute(query)
                logger.debug(f"Truncated table {self.table_name}")

            columns = batch.columns.tolist()
            column_names = ",".join(columns)
            placeholders = ",".join(["%s"] * len(columns))

            query = f"INSERT INTO {self.table_name} ({column_names}) VALUES ({placeholders})"

            data = [tuple(row) for row in batch.values]

            cursor.executemany(query, data)
            self.connection.commit()

            return cursor.rowcount

        except Exception as e:
            self.connection.rollback()
            logger.error(f"Replace failed: {e}")
            raise
        finally:
            cursor.close()

    def table_exists(self) -> bool:
        """Перевірити чи таблиця існує"""
        cursor = self.connection.cursor()

        try:
            query = """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = %s
                )
            """
            cursor.execute(query, (self.table_name,))
            return cursor.fetchone()[0]
        finally:
            cursor.close()

    def close(self):
        """Закрити коннекцію"""
        if self.connection:
            self.connection.close()
            logger.info(f"Closed connection for table {self.table_name}")
