# Production Apache Airflow для цього ETL-проєкту

## 1. Цільова ідея

Поточний ETL вже має нормальне розділення:

```text
extract -> transform -> load -> pipeline
```

Для Airflow це добре. Не треба переписувати все з нуля.

Production-ідея:

```text
Airflow DAG -> task -> src.pipelines.* -> PostgreSQL
```

Airflow не має містити бізнес-логіку ETL. Він має:

- запускати pipeline-и;
- задавати schedule;
- контролювати retries;
- показувати логи;
- робити backfill;
- показувати статуси запусків;
- керувати залежностями між task-ами.

## 2. Production Airflow: з чого складається

```text
User / Admin
  -> Airflow UI / API
  -> Scheduler
  -> DAG Processor
  -> Executor
  -> Workers
  -> Tasks
  -> ETL code
  -> Target PostgreSQL

Airflow Metadata DB
  зберігає DAG runs, task instances, users, connections, variables

Broker
  потрібен для CeleryExecutor: Redis або RabbitMQ
```

## 3. Airflow компоненти

| Компонент | Що робить | Django-аналогія |
|---|---|---|
| `DAG` | Опис workflow: task-и, порядок, schedule. | `urls.py` + частково `views.py`, бо описує маршрут виконання. |
| `Task` | Одна одиниця роботи. | Одна view/service-функція. |
| `Operator` | Тип task-а: Python, Bash, SQL, Docker тощо. | Class-Based View або готовий generic view. |
| `Scheduler` | Дивиться DAG-и й вирішує, що запускати. | Celery Beat / cron-диспетчер. |
| `Worker` | Фактично виконує task. | Celery worker. |
| `Executor` | Визначає, як запускати task-и. | Налаштування Celery backend/concurrency. |
| `Metadata DB` | Внутрішня БД Airflow. | Django DB для auth/admin/sessions/migrations. |
| `Connections` | Збережені доступи до БД/API. | `settings.py` + secret manager. |
| `Variables` | Runtime-конфіг для DAG-ів. | Django settings, але змінюються через admin. |
| `Pools` | Ліміти паралельних запусків. | Rate limit / semaphore. |
| `XCom` | Маленькі повідомлення між task-ами. | Передача короткого результату між service-функціями. |
| `Plugins` | Розширення Airflow. | Django apps / custom admin extensions. |
| `Hooks` | Обгортки для підключень до сервісів. | DB/API clients. |
| `Providers` | Пакети інтеграцій: Postgres, AWS, Slack тощо. | Django third-party apps. |

## 4. Production executor-и

| Executor | Коли використовувати | Коментар |
|---|---|---|
| `LocalExecutor` | Маленький prod або staging на одній машині. | Простіше, але без горизонтального масштабування. |
| `CeleryExecutor` | Production на Docker/VM з кількома workers. | Потрібен Redis/RabbitMQ. |
| `KubernetesExecutor` | Production у Kubernetes. | Кожен task може стартувати як окремий pod. |
| `CeleryKubernetesExecutor` | Гібрид для різних типів workload. | Складніше, але гнучко. |

Для вашого проєкту:

- локально: `LocalExecutor` або `CeleryExecutor`;
- production без Kubernetes: `CeleryExecutor`;
- production з Kubernetes: Helm Chart + `KubernetesExecutor` або `CeleryExecutor`.

## 5. Важливе production-правило

Не використовувати SQLite в production.

Airflow Metadata DB має бути:

- PostgreSQL;
- або MySQL.

Для вас логічно PostgreSQL, бо він уже є в проєкті.

Але краще мати окрему БД:

```text
airflow_metadata_db
```

Не змішувати її з:

- `etl_db`;
- `currency_db`;
- target-таблицями ETL.

## 6. Production структура репозиторію

Рекомендована структура:

```text
etl_project/
  airflow/
    config/
      airflow.cfg
      webserver_config.py
    logs/
    plugins/
    requirements/
      requirements-airflow.txt

  dags/
    jsonplaceholder/
      jsonplaceholder_dag.py
    currency/
      currency_dag.py

  docker/
    airflow/
      Dockerfile
    postgres/
      init/

  scripts/
    airflow_init.ps1
    airflow_db_migrate.ps1
    airflow_create_admin.ps1

  src/
    connectors/
    extract/
    load/
    loaders/
    pipelines/
    transform/

  tests/
    unit/
    dags/

  .env
  .env.example
  .env.airflow.example
  .gitignore
  docker-compose.yml
  docker-compose.airflow.yml
  config.py
  main.py
  requirements.txt
  README.md
```

## 7. Значення директорій

| Директорія | Що означає | Як використовується | Django-аналогія |
|---|---|---|---|
| `src/` | Основний ETL-код. | Airflow імпортує функції звідси. | Django apps/services. |
| `src/extract/` | Забір сирих даних. | Викликається всередині pipeline. | Service/API client layer. |
| `src/transform/` | Підготовка даних. | Не має знати про Airflow. | Business logic / serializers. |
| `src/load/` | Завантаження таблиць. | Пише у target PostgreSQL. | Repository/service layer. |
| `src/loaders/` | Універсальні loader-и. | Спільний механізм insert/upsert. | DB abstraction layer. |
| `src/connectors/` | Підключення до БД. | Створює PostgreSQL connection. | `DATABASES` + DB client. |
| `src/pipelines/` | Готові ETL-сценарії. | Airflow task викликає pipeline-функцію. | Django management commands / Celery tasks. |
| `dags/` | Airflow workflow-и. | Scheduler парсить ці файли. | `urls.py` для workflow-ів. |
| `airflow/config/` | Конфіг Airflow. | Airflow читає налаштування. | Django `settings.py`. |
| `airflow/logs/` | Логи Airflow task-ів. | Пише scheduler/worker. | Django logs. |
| `airflow/plugins/` | Розширення Airflow. | Custom operators/hooks/views. | Django reusable apps. |
| `docker/` | Dockerfile-и й init-скрипти. | Будує production images. | Deployment config. |
| `scripts/` | Команди для init/migrate/admin. | Запуск службових дій. | `manage.py` helper scripts. |
| `tests/` | Тести ETL і DAG-ів. | CI перевіряє до deploy. | Django tests. |

## 8. Значення production-файлів

| Файл | Що робить |
|---|---|
| `docker-compose.airflow.yml` | Описує Airflow services: scheduler, web/api, worker, triggerer, metadata DB, broker. |
| `docker/airflow/Dockerfile` | Створює Airflow image з ETL-кодом і залежностями. |
| `airflow/requirements/requirements-airflow.txt` | Залежності, які треба встановити в Airflow image. |
| `.env.airflow.example` | Приклад змінних для Airflow. |
| `dags/jsonplaceholder/jsonplaceholder_dag.py` | DAG для JSONPlaceholder pipeline. |
| `dags/currency/currency_dag.py` | DAG для currency pipeline. |
| `scripts/airflow_db_migrate.ps1` | Запускає міграції metadata DB. |
| `scripts/airflow_create_admin.ps1` | Створює admin-користувача Airflow. |
| `tests/dags/` | Перевіряє, що DAG-и імпортуються без помилок. |

## 9. Як мають виглядати DAG-и

Початковий правильний варіант:

```text
1 DAG = 1 business workflow
1 task = 1 існуючий pipeline
```

Для вашого проєкту:

```text
jsonplaceholder_daily
  -> run_jsonplaceholder_etl

currency_daily
  -> run_dim_currency_etl
  -> run_fact_exchange_rate_etl
```

Не варто одразу дробити на:

```text
extract -> transform -> load
```

Чому:

- більше DAG-коду;
- більше XCom;
- більше шансів зламати просту логіку;
- у вас pipeline-и вже написані.

Дробити треба пізніше, коли потрібні окремі retries для extract/load.

## 10. Django-аналогія на вашому проєкті

```text
Django project                    Airflow ETL project
--------------------------------------------------------------
settings.py                       config.py + airflow.cfg
urls.py                           dags/*.py
views.py                          pipeline task functions
services.py                       src/extract + src/transform + src/load
models.py                         schema.sql + currency_schema.sql
management commands               main.py
Celery tasks                      Airflow tasks
Celery beat                       Airflow scheduler
Django admin                      Airflow UI
DATABASES                         Airflow Connections + .env
migrations                        airflow db migrate + SQL schema files
installed apps                    providers/plugins
```

Коротко:

- `dags/` — це не місце для всієї логіки.
- `dags/` — це маршрутизація workflow-ів.
- ваша ETL-логіка має жити в `src/`.
- Airflow має тільки викликати `src`.

## 11. Production Docker Compose структура

Для production-like Docker Compose:

```text
services:
  airflow-scheduler
  airflow-worker
  airflow-triggerer
  airflow-webserver або airflow-api-server
  airflow-init
  airflow-postgres
  airflow-redis
  postgres
  postgres_currency
```

Пояснення:

| Service | Що робить |
|---|---|
| `airflow-scheduler` | Планує DAG runs і task instances. |
| `airflow-worker` | Виконує task-и. |
| `airflow-triggerer` | Обробляє deferrable tasks/sensors. |
| `airflow-webserver` / `airflow-api-server` | UI/API для керування Airflow. |
| `airflow-init` | Міграції metadata DB і створення admin user. |
| `airflow-postgres` | Metadata DB Airflow. |
| `airflow-redis` | Broker для CeleryExecutor. |
| `postgres` | Target DB для JSONPlaceholder. |
| `postgres_currency` | Target DB для валют. |

Важливо:

- `airflow-postgres` не має бути тією самою БД, що target ETL.
- target PostgreSQL і metadata PostgreSQL краще розділити.

## 12. Environment variables

Для local запуску зараз:

```env
JSONPLACEHOLDER_DB_HOST=localhost
POSTGRES_CURRENCY_DB_HOST=localhost
```

Для Airflow у Docker:

```env
JSONPLACEHOLDER_DB_HOST=postgres
POSTGRES_CURRENCY_DB_HOST=postgres_currency
```

Чому:

- `localhost` всередині контейнера означає сам контейнер;
- Docker service name означає інший контейнер у compose network.

## 13. Secrets у production

Мінімально:

```text
.env.airflow
```

Краще:

```text
Airflow Connections
```

Ще краще:

```text
Vault / AWS Secrets Manager / GCP Secret Manager / Kubernetes Secrets
```

Для вашого етапу достатньо:

- `.env` для local ETL;
- `.env.airflow` для Airflow;
- `.env.example` без паролів;
- `.env.airflow.example` без паролів.

## 14. Логи

Зараз:

```text
etl.log
```

У production Airflow:

```text
airflow/logs/
```

Краще для реального production:

- remote logging;
- S3 / GCS / Azure Blob;
- або centralized logging: Loki, ELK, CloudWatch.

Для вашого старту:

- не писати ETL-логи у файл;
- логувати через `logging.getLogger(__name__)`;
- дозволити Airflow збирати stdout/stderr.

## 15. Retry policy

Зараз retry є в extract-функціях.

В Airflow треба мати ще task-level retry:

```text
retries=3
retry_delay=5 minutes
execution_timeout=30 minutes
```

Правило:

- API retry — для тимчасових HTTP-помилок;
- Airflow retry — для падіння всього task-а.

## 16. Backfill

Зараз у `main.py` hardcoded:

```text
2024-01-01 -> 2024-12-31
```

У Airflow краще:

```text
logical_date -> один день даних
catchup=True -> історичні дати
manual trigger params -> ручний діапазон
```

Для `fact_exchange_rate` production-підхід:

```text
один DAG run = одна дата курсів
```

Не треба в одному task ганяти весь 2024 рік, якщо це може зробити Airflow backfill.

## 17. Idempotency

Airflow task може запускатись повторно.

Тому load має бути ідемпотентний:

```text
upsert або delete+insert for data interval
```

У вас уже добре:

- `users` upsert by `user_id`;
- `posts` upsert by `post_id`;
- `comments` upsert by `comment_id`;
- `fact_exchange_rate` upsert by `rate_date + currency_id`.

Це правильна база для Airflow.

## 18. Тести для production

Додати мінімум:

```text
tests/
  unit/
    test_transform_users.py
    test_transform_posts.py
    test_transform_comments.py
    test_transform_currency.py
  dags/
    test_dag_imports.py
```

Що перевіряти:

- transform-функції;
- схеми колонок;
- DAG imports;
- що DAG-и мають очікувані task-и;
- що немає важкого коду на top-level у DAG-файлах.

## 19. Що не робити

Не робити так:

```text
dags/
  dag.py   # 500 рядків extract/transform/load логіки
```

Не зберігати:

- паролі в DAG-файлах;
- великі DataFrame в XCom;
- ETL-результати в Airflow metadata DB;
- production на SQLite;
- production на quick-start compose без security hardening.

## 20. Ваш поточний проєкт: що добре

| Що | Чому добре |
|---|---|
| Є `src/extract/`, `src/transform/`, `src/load/` | Логіка вже розділена. |
| Є `src/pipelines/` | Airflow може викликати готові функції. |
| Є upsert | Повторний запуск task-а не має дублювати дані. |
| Є Docker PostgreSQL | Легко підключити Airflow у ту ж network. |
| Є `.env` | Конфіги вже винесені з коду. |
| Є `main.py` | Зручно лишити як manual/debug runner. |

## 21. Ваш поточний проєкт: що не так / що змінити

### 21.1. Hardcoded backfill у `main.py`

Зараз:

```python
run_fact_exchange_rate_etl(start_date="2024-01-01", end_date="2024-12-31")
```

Проблема:

- Airflow сам вміє backfill;
- один великий task на рік складніше retry-нути;
- при падінні в середині року task повторить великий шматок.

Як змінити:

- `main.py` залишити для manual запуску;
- у DAG передавати одну дату;
- для історії використовувати Airflow catchup/backfill.

### 21.2. `localhost` у конфігах

Зараз fallback у `config.py`:

```python
"host": os.getenv(..., "localhost")
```

Проблема:

- у Docker-контейнері `localhost` не веде до PostgreSQL service.

Як змінити:

- для local залишити `localhost`;
- для Airflow `.env.airflow` задати service names:

```env
JSONPLACEHOLDER_DB_HOST=postgres
POSTGRES_CURRENCY_DB_HOST=postgres_currency
```

### 21.3. `LOG_FILE=etl.log`

Зараз:

```python
LOG_FILE = os.getenv("LOG_FILE", "etl.log")
```

Проблема:

- у Airflow краще логи task-а, а не окремий файл;
- файл у контейнері може загубитись або дублювати Airflow logs.

Як змінити:

- для Airflow прибрати `FileHandler`;
- залишити `StreamHandler`;
- або зробити `LOG_FILE` optional.

### 21.4. `logging.basicConfig` у кожному pipeline

Зараз logging налаштовується в кількох файлах:

- `src/pipelines/jsonplaceholder.py`;
- `src/pipelines/dim_currency.py`;
- `src/pipelines/fact_exchange_rate.py`.

Проблема:

- Airflow сам налаштовує logging;
- повторний `basicConfig` може давати дублікати handler-ів.

Як змінити:

- винести logging setup у `main.py`;
- у pipeline-файлах залишити тільки:

```python
logger = logging.getLogger(__name__)
```

### 21.5. `BATCH_SIZE` є, але майже не використовується

Зараз:

```python
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 100))
```

А `PostgreSQLLoader` має default:

```python
batch_size=100
```

Проблема:

- env-змінна не керує loader-ом централізовано.

Як змінити:

- передавати `BATCH_SIZE` у `PostgreSQLLoader`;
- або встановити його в одному місці.

### 21.6. `utils/` не має ролі

Зараз:

- `utils/connector.py` — тестовий запит;
- `utils/db.py` — порожній.

Проблема:

- незрозуміло, чи це production-код.

Як змінити:

- видалити;
- або перенести в `examples/`;
- або зробити нормальний `src/utils/`, якщо реально потрібно.

### 21.7. SQL schema не є міграціями

Зараз:

```text
schema.sql
currency_schema.sql
```

Проблема:

- Docker init SQL виконується тільки при створенні volume;
- зміни схем пізніше не застосуються автоматично.

Як змінити:

- для простого варіанту залишити як є;
- для production додати migrations:
  - Alembic;
  - або SQL migration scripts з versioning;
  - або окремий migration step у deploy.

### 21.8. Немає тестів

Проблема:

- DAG може впасти вже після deploy;
- transform може змінити колонки непомітно.

Як змінити:

- додати `tests/unit/`;
- додати `tests/dags/`;
- перевіряти імпорт DAG-ів у CI.

### 21.9. DAG-и ще не створені

Зараз Airflow є тільки в плані.

Що додати першим:

```text
dags/jsonplaceholder/jsonplaceholder_dag.py
dags/currency/currency_dag.py
```

Перший варіант:

```text
1 pipeline = 1 task
```

Це найменш болючий шлях.

## 22. Мінімальний production-ready шлях

### Етап 1. Production-like local

Додати:

```text
docker-compose.airflow.yml
Dockerfile.airflow
requirements-airflow.txt
dags/
airflow/logs/
airflow/plugins/
airflow/config/
```

Ціль:

- Airflow UI відкривається;
- DAG-и імпортуються;
- task-и пишуть у PostgreSQL.

### Етап 2. Нормалізувати код

Змінити:

- прибрати hardcoded dates;
- зробити logging дружнім до Airflow;
- підключити `BATCH_SIZE`;
- прибрати `utils/`;
- додати `.env.example`.

### Етап 3. Production

Додати:

- окрему Airflow metadata DB;
- Redis/RabbitMQ для CeleryExecutor;
- remote logs;
- Airflow Connections;
- healthchecks;
- backups metadata DB;
- CI для DAG imports і unit tests.

### Етап 4. Реальний production

Краще:

- Kubernetes;
- Official Airflow Helm Chart;
- secrets manager;
- remote logging;
- monitoring/alerts;
- resource limits;
- separate workers by queue.

## 23. Короткий висновок

Ваш проєкт уже зручний для Airflow, бо ETL-логіка не змішана в одному файлі.

Найкраща стратегія:

```text
не переписувати ETL
додати DAG-и
запустити pipeline-и як task-и
потім поступово чистити конфіги, logging, backfill і тести
```

## 24. Офіційні джерела

- Airflow Production Deployment: https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/production-deployment.html
- Airflow Docker Compose guide: https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html
- Airflow Best Practices: https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html
- Airflow DAG concepts: https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html
