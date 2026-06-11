# Архітектура проєкту (ETL)

## Загальний опис

ETL-платформа: дані витягуються з зовнішніх API, очищаються/валідуються через Pandas
і завантажуються в PostgreSQL. Запускаються три незалежні пайплайни:

1. **jsonplaceholder** — users / posts / comments з JSONPlaceholder API → БД `jsonplaceholder`.
2. **dim_currency** — довідник валют з API НБУ → БД `postgres_currency`.
3. **fact_exchange_rate** — факт-таблиця курсів валют (з підтримкою backfill за діапазон дат) → БД `postgres_currency`.

Кожен пайплайн будується за схемою: **Extract → Transform → Load**.
Дві окремі бази PostgreSQL піднімаються в Docker.

## Потік даних

```
API → extract/ → transform/ (Pandas: clean + validate) → load/ → PostgreSQLLoader → PostgreSQL
                                  пайплайни в pipelines/ оркеструють кроки
```

## Структура каталогів і файлів

### Корінь

| Файл | За що відповідає |
|------|------------------|
| `main.py` | Точка входу. `run_all()` послідовно запускає всі три пайплайни, друкує результати, повертає exit code 1 якщо хоч один впав. |
| `config.py` | Конфіг із `.env`. Словник `DATABASES` (по одному запису на джерело: `jsonplaceholder`, `postgres_currency`), `BATCH_SIZE`, налаштування логів. |
| `docker-compose.yml` | Два сервіси PostgreSQL (`postgres`, `postgres_currency`) з окремими volume, healthcheck та init-скриптами. |
| `schema.sql` | DDL для БД jsonplaceholder: таблиці `users`, `posts`, `comments`. |
| `currency_schema.sql` | DDL для БД валют: `dim_currency`, `fact_exchange_rate` + індекси. |
| `requirements.txt` | Python-залежності. |
| `README.md` | Інструкція запуску, опис фаз ETL, схеми БД. |
| `AGENTS.md` | Нотатки/інструкції для агентів. |
| `.gitignore` | Ігноровані файли (`.env`, логи тощо). |

### `src/extract/` — витяг сирих даних з API

| Файл | За що відповідає |
|------|------------------|
| `users.py` | Запит users з JSONPlaceholder (retry-логіка). |
| `posts.py` | Запит posts. |
| `comments.py` | Запит comments. |
| `fact_exchange_rate.py` | Запит курсів валют з API НБУ; приймає `rate_date` для конкретної дати. |
| `__init__.py` | Маркер пакета. |

### `src/transform/` — очищення та валідація (Pandas)

| Файл | За що відповідає |
|------|------------------|
| `users.py` | Normalize JSON, вибір/перейменування колонок, видалення NULL/дублів, валідація email/id, type casting. |
| `posts.py` | Трансформація posts. |
| `comments.py` | Трансформація comments. |
| `dim_currency.py` | Формує довідник валют (currency_id/code/name) з відповіді API. |
| `fact_exchange_rate.py` | Формує факт-рядки курсів (rate_date, currency_id, rate). |
| `__init__.py` | Маркер пакета. |

### `src/load/` — завантаження в БД (обгортки над loader)

Кожен файл бере DataFrame + connection, створює `PostgreSQLLoader` з потрібним
ім'ям таблиці та primary key, викликає `load(mode=...)`.

| Файл | Таблиця |
|------|---------|
| `users.py` | `users` (PK `user_id`) |
| `posts.py` | `posts` |
| `comments.py` | `comments` |
| `dim_currency.py` | `dim_currency` |
| `fact_exchange_rate.py` | `fact_exchange_rate` |
| `__init__.py` | Маркер пакета. |

### `src/loaders/` — реалізація завантаження

| Файл | За що відповідає |
|------|------------------|
| `base_loader.py` | Абстрактний `BaseLoader` (ABC): контракт `load` / `table_exists` / `close`. |
| `postgresql_loader.py` | `PostgreSQLLoader`: батчеве завантаження DataFrame у PostgreSQL у режимах `insert` / `upsert` (ON CONFLICT) / `replace` (TRUNCATE+INSERT). |

### `src/connectors/` — підключення до БД

| Файл | За що відповідає |
|------|------------------|
| `postgresql.py` | `PostgreSQLConnector`: керування з'єднанням (connect/close, context manager) і виконання запитів (`execute_query`, `execute_update`, `execute_batch`). |

### `src/pipelines/` — оркестрація E→T→L

| Файл | За що відповідає |
|------|------------------|
| `jsonplaceholder.py` | `run_jsonplaceholder_etl()` — послідовно проганяє users, posts, comments; налаштовує логування; повертає статус. |
| `dim_currency.py` | `run_dim_currency_etl()` — ETL довідника валют. |
| `fact_exchange_rate.py` | `run_fact_exchange_rate_etl(start_date, end_date)` — ETL курсів; `iter_dates()` для backfill за діапазон дат; перед фактами оновлює `dim_currency`. |

### `utils/` — допоміжне / чернетки

| Файл | За що відповідає |
|------|------------------|
| `connector.py` | Чернетковий скрипт-приклад запиту до API (не використовується пайплайнами). |
| `db.py` | Порожній. |

### Untracked-документація

| Файл | За що відповідає |
|------|------------------|
| `ARCHITECTURE.md` | Чернетка опису архітектури. |
| `AIRFLOW_MIGRATION_PLAN.md` | План міграції на Apache Airflow. |
| `PRODUCTION_AIRFLOW_GUIDE.md` | Гайд із продакшн-розгортання Airflow. |

## Бази даних

- **jsonplaceholder**: `users` → `posts` (FK user_id) → `comments` (FK post_id).
- **postgres_currency**: `dim_currency` (довідник) ← `fact_exchange_rate` (факти, складений PK `rate_date + currency_id`).

## Режими завантаження (`PostgreSQLLoader`)

- `insert` — лише нові рядки.
- `upsert` — INSERT, при конфлікті PK — UPDATE (використовується скрізь за замовчуванням).
- `replace` — TRUNCATE таблиці + повне перезавантаження.
