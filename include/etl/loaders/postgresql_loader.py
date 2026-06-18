import pandas as pd
import logging
from typing import Dict, Any, List, Union
from psycopg2 import sql
from psycopg2.extras import execute_values

from etl.loaders.base_loader import BaseLoader

logger = logging.getLogger(__name__)


class PostgreSQLLoader(BaseLoader):
    """
    Loader для PostgreSQL.
    Підтримує режими: insert, upsert, replace.

    ВАЖЛИВО про транзакції:
    Транзакцією керує ТІЛЬКИ метод load() — він робить один commit у кінці
    або rollback при будь-якій помилці. Батч-методи (_*_batch) лише виконують
    SQL у межах поточної транзакції і НЕ комітять самі. Це гарантує принцип
    "все або нічого": якщо вантаження впаде на середині, у БД не лишиться
    частково завантажених даних.
    """

    def __init__(
        self,
        connection,
        table_name: str,
        primary_key: Union[str, List[str]] = "id",
        batch_size: int = 1000,
        owns_connection: bool = False,
    ):
        super().__init__(table_name)
        self.connection = connection
        self.primary_key = primary_key
        self.batch_size = batch_size
        # owns_connection=False (дефолт): з'єднання передане ззовні (PostgresHook,
        # пул) — loader його НЕ закриває, бо ним користуються інші.
        # owns_connection=True: loader сам володіє з'єднанням і має право закрити.
        self.owns_connection = owns_connection

    def load(self, df: pd.DataFrame, mode: str = "upsert") -> Dict[str, Any]:
        """
        Завантажити DataFrame у PostgreSQL.
        mode: 'insert', 'upsert', 'replace'

        Усі батчі виконуються в ОДНІЙ транзакції: commit лише після
        успіху всіх батчів, rollback при будь-якій помилці.
        """
        if df is None or df.empty:
            logger.warning(f"DataFrame is empty for {self.table_name}")
            return {"status": "warning", "rows": 0}

        logger.info(f"Loading {len(df)} rows to {self.table_name} (mode: {mode})")

        # Розділити на батчі (рознесення в часі для великих обсягів,
        # але всі — в межах однієї транзакції).
        batches = [df.iloc[i : i + self.batch_size] for i in range(0, len(df), self.batch_size)]

        try:
            total_rows = 0

            for batch_num, batch in enumerate(batches):
                # replace: TRUNCATE робимо ОДИН раз перед першим батчем,
                # у тій самій транзакції (без окремого commit).
                truncate_first = (mode == "replace" and batch_num == 0)
                rows_loaded = self._load_batch(batch, mode, truncate_first=truncate_first)
                total_rows += rows_loaded
                logger.debug(f"Batch {batch_num + 1}/{len(batches)}: {rows_loaded} rows")

            # Єдиний commit — після того, як ВСІ батчі пройшли успішно.
            self.connection.commit()
            logger.info(f"[OK] Loaded {total_rows} rows to {self.table_name}")

            return {"status": "success", "rows": total_rows, "table": self.table_name}

        except Exception as e:
            # Будь-яка помилка → відкочуємо ВСЮ транзакцію (всі батчі).
            self.connection.rollback()
            logger.error(f"[ERROR] Load failed, transaction rolled back: {e}", exc_info=True)
            raise

    def _load_batch(self, batch: pd.DataFrame, mode: str, truncate_first: bool = False) -> int:
        """Завантажити один батч у межах поточної транзакції (без commit)."""
        if mode == "insert":
            return self._insert_batch(batch)
        elif mode == "upsert":
            return self._upsert_batch(batch)
        elif mode == "replace":
            return self._replace_batch(batch, truncate_first=truncate_first)
        else:
            raise ValueError(f"Unknown mode: {mode}")

    def _table_identifier(self) -> sql.Identifier:
        """
        Безпечний ідентифікатор таблиці. Підтримує і 'table', і 'schema.table'.
        sql.Identifier екранує імена — захист від SQL-injection і від назв
        із спецсимволами/регістром.
        """
        parts = self.table_name.split(".")
        return sql.Identifier(*parts)

    @staticmethod
    def _rows(batch: pd.DataFrame) -> list[tuple]:
        """
        Перетворити DataFrame у список кортежів для вставки.

        Чому НЕ batch.values:
        - .values зводить увесь DataFrame до одного спільного dtype — integer
          з пропуском стає float (123 -> 123.0);
        - пропуски лишаються як NaN/NaT, і psycopg2 пише їх як 'NaN' →
          падіння або сміття в integer/date/text колонках.
        itertuples зберігає типи поколонково, а явна заміна NA -> None дає
        коректний SQL NULL.
        """
        return [
            tuple(None if pd.isna(v) else v for v in row)
            for row in batch.itertuples(index=False, name=None)
        ]

    def _insert_batch(self, batch: pd.DataFrame) -> int:
        """INSERT режим (без commit — керує load())."""
        cursor = self.connection.cursor()
        try:
            columns = batch.columns.tolist()

            # Композиція запиту через psycopg2.sql — імена екрануються коректно.
            query = sql.SQL("INSERT INTO {table} ({cols}) VALUES %s").format(
                table=self._table_identifier(),
                cols=sql.SQL(", ").join(map(sql.Identifier, columns)),
            )

            data = self._rows(batch)
            # execute_values вставляє всі рядки одним запитом — значно швидше за executemany.
            execute_values(cursor, query, data)
            return len(data)
        finally:
            cursor.close()

    def _upsert_batch(self, batch: pd.DataFrame) -> int:
        """UPSERT режим (INSERT ... ON CONFLICT DO UPDATE), без commit."""
        cursor = self.connection.cursor()
        try:
            columns = batch.columns.tolist()
            primary_keys = self.primary_key if isinstance(self.primary_key, list) else [self.primary_key]

            # Колонки для оновлення = всі, крім ключів конфлікту.
            update_cols = [c for c in columns if c not in primary_keys]

            if update_cols:
                conflict_action = sql.SQL("DO UPDATE SET {set_clause}").format(
                    set_clause=sql.SQL(", ").join(
                        sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(c))
                        for c in update_cols
                    )
                )
            else:
                # Усі колонки входять у PK — оновлювати нічого. Порожній SET — це
                # синтаксична помилка, тому в цьому разі робимо DO NOTHING.
                conflict_action = sql.SQL("DO NOTHING")

            query = sql.SQL(
                "INSERT INTO {table} ({cols}) VALUES %s "
                "ON CONFLICT ({pk}) {conflict_action}"
            ).format(
                table=self._table_identifier(),
                cols=sql.SQL(", ").join(map(sql.Identifier, columns)),
                pk=sql.SQL(", ").join(map(sql.Identifier, primary_keys)),
                conflict_action=conflict_action,
            )

            data = self._rows(batch)
            execute_values(cursor, query, data)
            return len(data)
        finally:
            cursor.close()

    def _replace_batch(self, batch: pd.DataFrame, truncate_first: bool = False) -> int:
        """
        REPLACE режим. TRUNCATE робиться лише раз (truncate_first=True) у тій
        самій транзакції, що й INSERT — тому таблиця ніколи не лишається
        порожньою при збої (rollback поверне і TRUNCATE).
        """
        cursor = self.connection.cursor()
        try:
            if truncate_first:
                cursor.execute(
                    sql.SQL("TRUNCATE TABLE {table} CASCADE").format(table=self._table_identifier())
                )
                logger.debug(f"Truncated table {self.table_name} (in transaction)")

            columns = batch.columns.tolist()
            query = sql.SQL("INSERT INTO {table} ({cols}) VALUES %s").format(
                table=self._table_identifier(),
                cols=sql.SQL(", ").join(map(sql.Identifier, columns)),
            )

            data = self._rows(batch)
            execute_values(cursor, query, data)
            return len(data)
        finally:
            cursor.close()

    def table_exists(self) -> bool:
        """Перевірити чи таблиця існує."""
        cursor = self.connection.cursor()
        try:
            # Беремо лише останню частину імені (без схеми) для information_schema.
            table_only = self.table_name.split(".")[-1]
            cursor.execute(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = %s
                )
                """,
                (table_only,),
            )
            return cursor.fetchone()[0]
        finally:
            cursor.close()

    def close(self):
        """
        Закрити коннекцію — ЛИШЕ якщо loader нею володіє (owns_connection=True).
        Інакше (з'єднання з PostgresHook/пулу) close() — no-op, щоб не зламати
        тих, хто ще користується цим з'єднанням ("connection already closed").
        """
        if self.connection and self.owns_connection:
            self.connection.close()
            logger.info(f"Closed connection for table {self.table_name}")
