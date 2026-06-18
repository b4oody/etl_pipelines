# План інтеграції Apache Airflow

## Ціль

Додати Apache Airflow без переписування ETL-логіки.

Правильна схема:

```text
Airflow DAG -> src.pipelines.* -> PostgreSQL
```

Тобто Airflow тільки запускає готові pipeline-функції.

## Що залишити

| Що | Рішення |
|---|---|
| `src/extract/` | Залишити без змін. |
| `src/transform/` | Залишити без змін. |
| `src/load/` | Залишити без змін. |
| `src/loaders/` | Залишити без змін. |
| `src/connectors/` | Залишити без змін. |
| `src/pipelines/` | Залишити, Airflow буде викликати ці функції. |
| `main.py` | Залишити для ручного запуску й дебагу. |
| `docker-compose.yml` | Залишити для PostgreSQL. |

## Що додати

### Директорії

```text
dags/
airflow/
  config/
  logs/
  plugins/
scripts/
```

### Файли

| Файл | Навіщо |
|---|---|
| `dags/jsonplaceholder_dag.py` | DAG для users/posts/comments. |
| `dags/currency_dag.py` | DAG для `dim_currency` і `fact_exchange_rate`. |
| `docker-compose.airflow.yml` | Airflow webserver, scheduler, metadata DB. |
| `Dockerfile.airflow` | Airflow image з Python-залежностями проєкту. |
| `requirements-airflow.txt` | Додаткові залежності для Airflow. |
| `.env.example` | Приклад env для ETL. |
| `.env.airflow.example` | Приклад env для Airflow. |
| `scripts/init_airflow.ps1` | Підготовка папок і стартова ініціалізація Airflow. |

## Що встановити / завантажити

Потрібно:

- Docker Desktop.
- Docker Compose.
- Docker image `apache/airflow`.
- `apache-airflow-providers-postgres`.
- Поточні залежності з `requirements.txt`.

У `requirements-airflow.txt`:

```text
pandas
requests
psycopg2-binary
python-dotenv
apache-airflow-providers-postgres
```

Airflow version краще фіксувати явно у `Dockerfile.airflow`, не брати `latest`.

## Що змінити

### `docker-compose.yml`

Не замінювати.

Залишити тільки PostgreSQL:

- `postgres`;
- `postgres_currency`;
- volumes;
- init SQL.

Airflow винести в окремий файл:

```text
docker-compose.airflow.yml
```

Запуск:

```bash
docker-compose -f docker-compose.yml -f docker-compose.airflow.yml up -d
```

### `config.py`

Зараз можна залишити.

Для Airflow у Docker важливо не використовувати `localhost` для PostgreSQL.

У `.env.airflow.example` краще вказати:

```env
JSONPLACEHOLDER_DB_HOST=postgres
POSTGRES_CURRENCY_DB_HOST=postgres_currency
```

### `main.py`

Не видаляти.

Залишити як manual runner.

Пізніше можна:

- прибрати hardcoded backfill;
- додати CLI-аргументи;
- дозволити запуск одного pipeline.

### `.gitignore`

Додати:

```gitignore
airflow/logs/
airflow/*.pid
```

### `README.md`

Додати короткий розділ:

- як запустити Airflow;
- як відкрити UI;
- які DAG-и є;
- як запустити pipeline вручну.

## Що видалити

Одразу нічого не видаляти.

Після успішної інтеграції можна:

| Файл | Дія |
|---|---|
| `utils/connector.py` | Видалити або перенести в `examples/`. |
| `utils/db.py` | Видалити, якщо не буде використовуватись. |
| `etl.log` | Не тримати в Git, логи має вести Airflow. |

## Що заміниться

| Було | Стане |
|---|---|
| `python main.py` як основний запуск | Airflow DAG-и як основний запуск. |
| `run_all()` | Окремі DAG-и або task-и. |
| Локальний `etl.log` | Airflow task logs. |
| Hardcoded dates у `main.py` | Airflow params, catchup або backfill. |
| Ручний порядок запуску | Dependencies між task-ами. |

## Безболісний план

### Крок 1. Додати Airflow поруч

Не чіпати поточний ETL.

Результат:

- `python main.py` працює як раніше;
- Airflow додається як другий спосіб запуску.

### Крок 2. Додати Docker для Airflow

Додати:

- `docker-compose.airflow.yml`;
- `Dockerfile.airflow`;
- `requirements-airflow.txt`;
- `.env.airflow.example`;
- `airflow/logs/`;
- `airflow/plugins/`;
- `airflow/config/`.

Підключити код у контейнер:

```text
./src:/opt/airflow/src
./config.py:/opt/airflow/config.py
./dags:/opt/airflow/dags
```

### Крок 3. Додати перший DAG

Створити:

```text
dags/jsonplaceholder_dag.py
```

Перший варіант простий:

```text
one task -> run_jsonplaceholder_etl()
```

Ціль:

- DAG видно в UI;
- task запускається;
- дані пишуться в PostgreSQL;
- логи видно в Airflow.

### Крок 4. Додати currency DAG

Створити:

```text
dags/currency_dag.py
```

Порядок:

```text
run_dim_currency_etl >> run_fact_exchange_rate_etl
```

Спочатку запускати тільки поточний день.

### Крок 5. Прибрати hardcoded dates

У `main.py` зараз є:

```text
2024-01-01 -> 2024-12-31
```

Для Airflow краще:

- брати дату з `logical_date`;
- або передавати `start_date/end_date` через DAG params;
- або використовувати Airflow backfill.

### Крок 6. Потім розбити task-и

Не робити це одразу.

Почати з:

```text
1 pipeline = 1 task
```

Пізніше можна:

```text
extract -> transform -> load
```

Так легше дебажити, але більше коду в DAG.

### Крок 7. Перенести секрети

Після стабільного запуску:

- або залишити `.env` для локального проєкту;
- або перенести доступи в Airflow Connections.

Для навчального проєкту `.env` достатньо.

## Рекомендована фінальна структура

```text
etl_project/
  airflow/
    config/
    logs/
    plugins/
  dags/
    jsonplaceholder_dag.py
    currency_dag.py
  scripts/
    init_airflow.ps1
  src/
    connectors/
    extract/
    load/
    loaders/
    pipelines/
    transform/
  .env
  .env.example
  .env.airflow.example
  .gitignore
  AIRFLOW_MIGRATION_PLAN.md
  ARCHITECTURE.md
  Dockerfile.airflow
  docker-compose.yml
  docker-compose.airflow.yml
  config.py
  main.py
  requirements.txt
  requirements-airflow.txt
  schema.sql
  currency_schema.sql
  README.md
```

## Рекомендація

Рухатись маленькими кроками:

1. Додати Airflow Docker.
2. Запустити один DAG.
3. Підключити другий DAG.
4. Прибрати hardcoded dates.
5. Перенести секрети/логи/конфіги.

Так поточний ETL не ламається, а Airflow додається поступово.
