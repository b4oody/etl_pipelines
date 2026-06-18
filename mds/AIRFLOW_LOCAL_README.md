# Локальний запуск Airflow (etl_project)

Стек у `docker-compose.airflow.yml`. Окремий від твого ETL-стека (`docker-compose.yml`).
Airflow 3.2.2 (Python 3.12), LocalExecutor, власна Postgres-metadata БД.

## Передумова

Запусти **Docker Desktop** (зараз daemon не активний). Перевір:

```bash
docker info
```

## Перший запуск

```bash
docker compose -f docker-compose.airflow.yml --env-file .env.airflow up -d
```

Що відбувається по черзі:
1. `airflow-postgres` — піднімається metadata-БД, чекає healthcheck;
2. `airflow-init` — `airflow db migrate` + створює адміна, потім завершується (це нормально);
3. `airflow-apiserver` + `airflow-scheduler` — стартують і лишаються працювати.

Перший раз тягне образ (~1.5 ГБ) — зачекай кілька хвилин.

## Вхід в UI

- URL: http://localhost:8081
- Логін / пароль: `admin` / `admin` (з `.env.airflow`)

## Корисні команди

```bash
# статус
docker compose -f docker-compose.airflow.yml ps

# логи (стеж за стартом)
docker compose -f docker-compose.airflow.yml logs -f airflow-scheduler

# зупинити (дані лишаються)
docker compose -f docker-compose.airflow.yml down

# повний скид (видаляє metadata-БД!)
docker compose -f docker-compose.airflow.yml down -v
```

## Що далі

- `dags/` — сюди кладеш DAG-и (поки порожня).
- `include/` — сюди переїде твій `src/` як importable-пакет `etl/` (Етап 0 плану).
- Connection `postgres_currency` уже прокинутий через env у compose
  (`AIRFLOW_CONN_POSTGRES_CURRENCY`), вказує на твій currency-Postgres на `host.docker.internal:5433`.

> Примітка: щоб DAG-и достукались до твоєї БД курсів, спершу підніми ETL-Postgres:
> `docker compose -f docker-compose.yml --env-file .env up -d postgres_currency`
