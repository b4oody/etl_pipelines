# Поточна архітектура проєкту

## Загальна схема

```text
API -> Extract -> Transform -> Load -> PostgreSQL
```

Проєкт має два ETL-напрями:

- `jsonplaceholder` — users, posts, comments з JSONPlaceholder API.
- `postgres_currency` — валюти та курси валют з API НБУ.

Запуск зараз виконується вручну через `main.py`.

## Структура

```text
etl_project/
  src/
    connectors/
    extract/
    load/
    loaders/
    pipelines/
    transform/
  utils/
  .env
  .gitignore
  AGENTS.md
  config.py
  currency_schema.sql
  docker-compose.yml
  main.py
  README.md
  requirements.txt
  schema.sql
```

## Директорії

| Директорія | Відповідальність |
|---|---|
| `src/connectors/` | Підключення до баз даних. |
| `src/extract/` | Отримання сирих даних з API. |
| `src/transform/` | Очистка, нормалізація, перейменування колонок, приведення типів. |
| `src/load/` | Завантаження конкретних таблиць у PostgreSQL. |
| `src/loaders/` | Універсальна логіка insert/upsert/replace. |
| `src/pipelines/` | Готові ETL-сценарії: extract + transform + load. |
| `utils/` | Допоміжні/тестові файли, зараз майже не використовуються. |

## Файли в корені

| Файл | Відповідальність |
|---|---|
| `.env` | Локальні змінні середовища: host, port, user, password, db name. |
| `.gitignore` | Ігнорує `.env`, `.venv`, логи, кеш Python, IDE-файли. |
| `AGENTS.md` | Інструкції для Codex/агента. |
| `README.md` | Основна документація по запуску та проєкту. |
| `requirements.txt` | Python-залежності ETL-проєкту. |
| `config.py` | Читає `.env` і формує конфіги баз даних. |
| `main.py` | Ручний запуск усіх ETL pipeline-ів. |
| `docker-compose.yml` | Піднімає дві PostgreSQL БД. |
| `schema.sql` | Схема для `users`, `posts`, `comments`. |
| `currency_schema.sql` | Схема для `dim_currency`, `fact_exchange_rate`. |
| `etl.log` | Локальний лог-файл, не має комітитись. |

## `src/connectors/`

| Файл | Відповідальність |
|---|---|
| `postgresql.py` | `PostgreSQLConnector`: connect, close, query, update, batch, rollback. |

## `src/extract/`

| Файл | Відповідальність |
|---|---|
| `users.py` | Забирає users з JSONPlaceholder. |
| `posts.py` | Забирає posts з JSONPlaceholder. |
| `comments.py` | Забирає comments з JSONPlaceholder. |
| `fact_exchange_rate.py` | Забирає курси валют з API НБУ, підтримує дату. |
| `__init__.py` | Робить директорію Python package. |

## `src/transform/`

| Файл | Відповідальність |
|---|---|
| `users.py` | Готує users: `user_id`, `name`, `username`, `email`, `city`, `company_name`. |
| `posts.py` | Готує posts: `post_id`, `user_id`, `title`, `body`. |
| `comments.py` | Готує comments: `comment_id`, `post_id`, `name`, `email`, `body`. |
| `dim_currency.py` | Готує довідник валют: `currency_id`, `currency_code`, `currency_name`. |
| `fact_exchange_rate.py` | Готує факти курсів: `currency_id`, `rate`, `rate_date`. |
| `__init__.py` | Робить директорію Python package. |

## `src/load/`

| Файл | Відповідальність |
|---|---|
| `users.py` | Завантажує таблицю `users`, ключ `user_id`. |
| `posts.py` | Завантажує таблицю `posts`, ключ `post_id`. |
| `comments.py` | Завантажує таблицю `comments`, ключ `comment_id`. |
| `dim_currency.py` | Завантажує таблицю `dim_currency`, ключ `currency_id`. |
| `fact_exchange_rate.py` | Завантажує таблицю `fact_exchange_rate`, ключ `rate_date + currency_id`. |
| `__init__.py` | Робить директорію Python package. |

## `src/loaders/`

| Файл | Відповідальність |
|---|---|
| `base_loader.py` | Абстрактний інтерфейс loader-а. |
| `postgresql_loader.py` | Batch insert/upsert/replace у PostgreSQL. |

## `src/pipelines/`

| Файл | Відповідальність |
|---|---|
| `jsonplaceholder.py` | Повний ETL для users -> posts -> comments. |
| `dim_currency.py` | ETL для довідника валют. |
| `fact_exchange_rate.py` | ETL для курсів валют, включно з діапазоном дат. |

## `utils/`

| Файл | Відповідальність |
|---|---|
| `connector.py` | Тестовий запит до JSONPlaceholder, не основний pipeline. |
| `db.py` | Порожній файл, зараз не використовується. |

## Поточний запуск

```bash
docker-compose up -d
python main.py
```

`main.py` запускає:

1. `run_jsonplaceholder_etl()`.
2. `run_dim_currency_etl()`.
3. `run_fact_exchange_rate_etl(start_date="2024-01-01", end_date="2024-12-31")`.
