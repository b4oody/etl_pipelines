# Apache Airflow на проді: повна архітектура, структура та аналогії з Django

> Гайд написаний під твій проєкт `etl_project` (E/T/L з модулями `extract` / `transform` / `load` / `pipelines`,
> конфіг через `config.DATABASES`, конектор `PostgreSQLConnector`, пайплайн курсів валют НБУ).
> Мета — щоб ти **розумів архітектуру**, а не просто скопіював код.

---

## Зміст

1. [Що таке Airflow і навіщо він замість `main.py`](#1)
2. [Ментальна модель: DAG, Task, Operator, Run](#2)
3. [Архітектура компонентів (хто за що відповідає)](#3)
4. [Структура проєкту на проді + аналогії з Django](#4)
5. [Конфіги: airflow.cfg, env, Connections, Variables, Pools](#5)
6. [Executors: як саме виконуються задачі](#6)
7. [Як писати DAG: 3 стилі + повний приклад на курсах валют](#7)
8. [Hooks, Operators, Sensors, XCom — словник](#8)
9. [Backfill, catchup, idempotency — головне для ETL](#9)
10. [Production deployment (Docker / K8s), CI/CD](#10)
11. [Моніторинг, логи, алерти, безпека](#11)
12. [Покроковий план міграції твого проєкту](#12)

---

<a name="1"></a>
## 1. Що таке Airflow і навіщо він замість `main.py`

Зараз у тебе:

```python
# main.py
def run_all():
    results["jsonplaceholder"] = run_jsonplaceholder_etl()
    results["dim_currency"] = run_dim_currency_etl()
    results["fact_exchange_rate"] = run_fact_exchange_rate_etl(start_date="2024-01-01", end_date="2024-12-31")
```

Проблеми такого підходу на проді:

| Проблема | Чому болить |
|---|---|
| Запуск вручну / через cron | Немає UI, немає історії, cron не знає про залежності |
| Впав `extract` — що далі? | Весь скрипт падає, ретраїв немає, треба перезапускати все |
| «Перезалий лише 5 березня» | У `main.py` доводиться лізти руками і правити дати |
| Дві задачі залежать одна від одної | Послідовність зашита в код, паралелізм важко зробити |
| Хто запустив, коли, скільки рядків? | Тільки в `etl.log`, грепати руками |

**Airflow** — це **оркестратор**. Він не робить ETL сам, він **керує запуском** твоїх функцій:
коли запускати, у якому порядку, що робити при падінні, скільки паралельно, як перезапустити за конкретну дату.

> Аналогія: твій `main.py` — це людина, що вручну натискає «запустити».
> Airflow — це менеджер, який має розклад, журнал, кнопку «повторити» і вміє рахувати залежності.

**Ключове правило:** Airflow — це **оркестрація**, а не місце для важких обчислень.
Логіку (твої `extract_*`, `transform_*`, `load_*`) ти **не переписуєш** — ти її **викликаєш** з DAG.

---

<a name="2"></a>
## 2. Ментальна модель: DAG, Task, Operator, Run

Чотири поняття, які треба засвоїти намертво:

```
DAG  ─────────────────────────────────────────────┐
  │  (граф: "курси валют, щодня о 09:00")           │
  │                                                 │
  ├── Task: extract  ──►  Task: transform  ──►  Task: load
  │   (вузол графа)       (вузол)               (вузол)
  │
  └── кожен Task = екземпляр Operator
      (Operator = "клас, що вміє щось робити":
       PythonOperator виконує функцію,
       BashOperator виконує bash,
       PostgresOperator виконує SQL)
```

- **DAG** (Directed Acyclic Graph) — **орієнтований ациклічний граф**. Це опис «що з чого складається і в якому порядку». Один DAG ≈ один твій pipeline (напр. `fact_exchange_rate`). Цикли заборонені (задача не може залежати сама від себе).
- **Task** — один вузол графа. Напр. «витягнути курси». Залежності: `extract >> transform >> load`.
- **Operator** — *шаблон* задачі. Task — це Operator із конкретними параметрами. `PythonOperator(task_id="extract", python_callable=extract_exchange_rates)`.
- **DAG Run** — один конкретний **запуск** DAG за конкретну дату (`logical_date`). «Запуск за 2024-03-05» — це окремий Run зі своїм статусом.
- **Task Instance (TI)** — один Task у межах одного Run. У нього є стан: `success / failed / running / up_for_retry / skipped`.

> Аналогія з Django: **DAG ≈ urls.py + view-функція разом узяті** — це опис маршруту й логіки.
> **DAG Run ≈ один HTTP-запит** до цього маршруту. **Task Instance ≈ виклик однієї функції в межах цього запиту.**

---

<a name="3"></a>
## 3. Архітектура компонентів (хто за що відповідає)

На проді Airflow — це **не один процес**, а кілька, що спілкуються через спільну БД.

```
                ┌────────────────────────────────────────────────┐
                │              Metadata Database                   │
                │   (PostgreSQL — стан DAG-ів, runs, XCom, users)  │
                └───────▲───────────────▲────────────────▲────────┘
                        │               │                │
          ┌─────────────┴───┐   ┌───────┴───────┐   ┌────┴─────────┐
          │   Scheduler     │   │   Webserver   │   │   Workers    │
          │ (мозок: коли    │   │ (UI на 8080,  │   │ (м'язи:      │
          │  що запускати,  │   │  REST API,    │   │  реально     │
          │  ставить у      │   │  логи, графи) │   │  виконують   │
          │  чергу)         │   │               │   │  твій код)   │
          └────────┬────────┘   └───────────────┘   └────▲─────────┘
                   │                                      │
                   └──────────►  Message Broker  ─────────┘
                                (Redis / RabbitMQ — черга задач,
                                 лише для CeleryExecutor)
                   ┌────────────┐
                   │  Triggerer │  (для async-сенсорів / deferrable operators)
                   └────────────┘
```

| Компонент | Що робить | Django-аналогія |
|---|---|---|
| **Metadata DB** | Зберігає **стан**: які DAG є, статуси runs, XCom, користувачі, конекшени. Сам код DAG-ів там НЕ зберігається. | Твоя app-база + таблиці Django (`django_migrations`, `auth_user`...). Це «пам'ять системи». |
| **Scheduler** | Читає файли DAG, рахує розклад, вирішує що пора запускати, ставить задачі в чергу, обробляє ретраї. **Серце системи.** | Cron + диспетчер. У Django прямого аналога нема — найближче `Celery beat`. |
| **Webserver** | Flask/FastAPI-застосунок: UI на `:8080`, граф, логи, кнопки Trigger/Clear, REST API. | `runserver` + Django Admin. UI для перегляду й керування. |
| **Worker** | Процес, що **реально виконує** код задачі. Їх може бути багато. | Celery worker. |
| **Triggerer** | Окремий async-процес для «deferrable» операторів (сенсори, що чекають, не займаючи worker-слот). | Немає прямого аналога. |
| **Message Broker** (Redis/RabbitMQ) | Черга між Scheduler і Worker. Потрібен лише для CeleryExecutor. | Celery broker. |
| **DAG files** | Твої `.py` файли з описом DAG-ів. Лежать у папці `dags/`. Scheduler їх **парсить періодично**. | `urls.py` + `views.py`: код, який система читає, щоб знати «що робити». |

**Найважливіше для розуміння:** Scheduler **парсить усі файли в `dags/` кожні ~30 секунд**.
Тому: (1) код на верхньому рівні DAG-файлу має бути **легким** (жодних `requests.get` поза функціями!),
(2) важка логіка — лише **всередині задач**, які виконує worker.

---

<a name="4"></a>
## 4. Структура проєкту на проді + аналогії з Django

Ось як виглядає **типовий прод-репозиторій** Airflow. Зліва — Airflow, справа — що це в термінах Django/твого проєкту.

```
airflow-project/
│
├── dags/                          ◄── СЕРЦЕ. Тут лежать DAG-и (схоже на Django "urls.py + views")
│   ├── exchange_rate_dag.py            один файл = один (або кілька) DAG
│   ├── jsonplaceholder_dag.py
│   └── common/                        спільні хелпери для DAG-ів (default_args, теги)
│       └── default_args.py
│
├── plugins/                       ◄── Кастомні Operators / Hooks / Sensors
│   ├── operators/                     твої власні оператори (як reusable Django mixin/CBV)
│   │   └── nbu_to_postgres_operator.py
│   ├── hooks/                         кастомні підключення до зовнішніх систем
│   │   └── nbu_hook.py
│   └── sensors/                       кастомні сенсори (очікування подій)
│
├── include/  (або src/, etl/)     ◄── ТВІЙ бізнес-код. Airflow його НЕ парсить як DAG.
│   └── etl/                            Сюди переїжджає твій теперішній src/
│       ├── extract/
│       │   └── fact_exchange_rate.py   (твій теперішній extract — БЕЗ ЗМІН)
│       ├── transform/
│       │   └── fact_exchange_rate.py
│       ├── load/
│       │   └── fact_exchange_rate.py
│       └── connectors/
│           └── postgresql.py
│
├── tests/                         ◄── DAG-тести + юніт-тести (як Django tests/)
│   ├── test_dag_integrity.py          перевірка що всі DAG парсяться без помилок
│   └── test_exchange_etl.py
│
├── config/
│   └── airflow.cfg                ◄── головний конфіг (як Django settings.py)
│
├── plugins_config/  variables.json    bulk-import Variables (опційно)
│
├── Dockerfile                     ◄── образ з твоїми залежностями
├── docker-compose.yml             ◄── локальний стек (webserver+scheduler+worker+db+redis)
├── requirements.txt               ◄── pip-залежності (pandas, requests, psycopg2...)
└── .env                           ◄── секрети й конфіг оточення
```

### Прямі аналогії Airflow ↔ Django

| Airflow | Django | Сенс |
|---|---|---|
| `dags/` | `urls.py` + `views.py` | Точки входу, які система знаходить і реєструє автоматично |
| один `*_dag.py` | один view + його маршрут | Опис «що робити і коли» |
| `airflow.cfg` | `settings.py` | Глобальна конфігурація системи |
| Metadata DB | App database | Стан, історія, користувачі |
| `plugins/operators/` | reusable mixins / CBV / template tags | Перевикористовувані будівельні блоки |
| `plugins/hooks/` | Django DB backends / API clients | Адаптери до зовнішніх систем |
| `include/etl/` (твій `src/`) | твої `services.py` / `utils.py` | Бізнес-логіка, яку викликають з view |
| Airflow Connections | `DATABASES` у settings.py + секрети | Підключення до БД/API в одному місці |
| Airflow Variables | `settings.py` константи / env | Параметри, які можна міняти без деплою |
| `airflow db migrate` | `manage.py migrate` | Накат схеми metadata-БД |
| `airflow users create` | `createsuperuser` | Створення адміна для UI |
| Webserver UI | Django Admin | Адмінка для перегляду й керування |
| Scheduler | (немає; ~Celery beat) | Планувальник, якого в Django нема з коробки |

> **Головна думка:** твій теперішній `src/` майже без змін стає `include/etl/`.
> Ти **не переписуєш ETL** — ти додаєш зверху тонкий шар `dags/`, який цей ETL **викликає за розкладом**.

---

<a name="5"></a>
## 5. Конфіги: airflow.cfg, env, Connections, Variables, Pools

Airflow має **кілька рівнів конфігурації** — важливо розуміти, що де живе.

### 5.1 `airflow.cfg` — глобальні налаштування (як `settings.py`)

Це INI-файл. Ключові секції:

```ini
[core]
# Папка з DAG-ами (Scheduler сканує саме її)
dags_folder = /opt/airflow/dags
# Який executor — головне архітектурне рішення (див. розділ 6)
executor = CeleryExecutor
# Скільки задач паралельно у всьому інстансі
parallelism = 32
# Скільки активних DAG-Run одночасно на ОДИН DAG
max_active_runs_per_dag = 16
# НЕ запускати всі пропущені рани при старті нового DAG
# (критично для ETL — інакше отримаєш сотні backfill-ранів)
catchup_by_default = False

[database]
# Metadata DB. Це окрема база ВІД твоєї ETL-бази!
sql_alchemy_conn = postgresql+psycopg2://airflow:airflow@postgres/airflow

[scheduler]
# Як часто Scheduler перечитує файли DAG (сек)
dag_dir_list_interval = 30
# Тайм-аут парсингу одного файлу
dagbag_import_timeout = 30

[webserver]
base_url = https://airflow.company.com
web_server_port = 8080
# Секрет для підпису сесій/токенів
secret_key = <із env, не хардкодити>

[celery]
broker_url = redis://redis:6379/0
result_backend = db+postgresql://airflow:airflow@postgres/airflow

[logging]
# На проді логи пишуть у S3/GCS, а не на диск воркера
remote_logging = True
remote_base_log_folder = s3://my-airflow-logs/
```

> **Будь-яку опцію з `airflow.cfg` можна задати через env-змінну** за схемою
> `AIRFLOW__<СЕКЦІЯ>__<КЛЮЧ>`. Напр. `executor` → `AIRFLOW__CORE__EXECUTOR=CeleryExecutor`.
> На проді (Docker/K8s) **завжди** конфігурують через env, а не редагуванням файлу.

### 5.2 Connections — підключення до БД та API (замість твого `config.DATABASES`)

Зараз у тебе підключення зашите в `config.py`:

```python
DATABASES = {
    "postgres_currency": {"host": ..., "user": ..., "password": ...},
}
```

В Airflow це переїжджає в **Connections** — централізоване сховище кредів (зберігаються
зашифровано в metadata DB). Можна додати через UI (Admin → Connections), CLI або env.

```bash
# Через CLI
airflow connections add postgres_currency \
    --conn-type postgres \
    --conn-host localhost \
    --conn-schema etl_db \
    --conn-login etl_user \
    --conn-password etl_password \
    --conn-port 5432

# Або через env (зручно для Docker; формат URI)
export AIRFLOW_CONN_POSTGRES_CURRENCY='postgresql://etl_user:etl_password@localhost:5432/etl_db'
```

Потім у коді:

```python
from airflow.providers.postgres.hooks.postgres import PostgresHook

hook = PostgresHook(postgres_conn_id="postgres_currency")
conn = hook.get_conn()   # звичайний psycopg2 connection — твій load_* код працює як є
```

> Аналогія: `config.DATABASES` = Django `DATABASES` у settings.
> Airflow Connections = те саме, але **винесене з коду в захищене сховище** + з UI для редагування.

### 5.3 Variables — параметри без деплою

Динамічні параметри (напр. дата старту backfill, список валют, фіча-флаги):

```python
from airflow.models import Variable

base_url = Variable.get("nbu_api_url", default_var="https://bank.gov.ua/...")
currencies = Variable.get("tracked_currencies", deserialize_json=True)  # ["USD","EUR"]
```

> Аналогія: константи в `settings.py`, але які можна змінити в UI **без передеплою**.

### 5.4 Pools — обмеження конкурентності

Якщо API НБУ не любить багато паралельних запитів — створюєш Pool `nbu_api` на 2 слоти,
і всі задачі, що ходять у НБУ, конкурують лише за ці 2 слоти. Захист від перевантаження джерела.

---

<a name="6"></a>
## 6. Executors: як саме виконуються задачі

**Executor** — це найважливіше архітектурне рішення. Він визначає **де і як** виконується код задач.

| Executor | Як працює | Коли використовувати |
|---|---|---|
| **SequentialExecutor** | Одна задача за раз, SQLite. | Тільки демо. Ніколи на проді. |
| **LocalExecutor** | Паралельні задачі як підпроцеси на **одній** машині. Потрібна PostgreSQL. | Малий/середній прод, один сервер. Чудовий старт для тебе. |
| **CeleryExecutor** | Задачі йдуть у чергу (Redis/RabbitMQ), виконуються **пулом воркерів** на різних машинах. | Класичний прод, горизонтальне масштабування. |
| **KubernetesExecutor** | Кожна задача = окремий **K8s pod**, що створюється на льоту і вмирає після. | Хмарний прод, ізоляція залежностей, еластичність. |
| **CeleryKubernetesExecutor** | Гібрид: легкі задачі через Celery, важкі — в окремих подах. | Великі команди зі змішаним навантаженням. |

```
LocalExecutor:                      CeleryExecutor:
┌──────────────────┐                ┌───────────┐   ┌─────────┐   ┌──────────┐
│  один сервер     │                │ Scheduler │──►│  Redis  │◄──│ Worker 1 │
│  Scheduler       │                └───────────┘   │ (черга) │   ├──────────┤
│   ├─ subprocess1 │                                └─────────┘◄──│ Worker 2 │
│   ├─ subprocess2 │                                              ├──────────┤
│   └─ subprocess3 │                                              │ Worker N │
└──────────────────┘                                             └──────────┘
```

> **Рекомендація для твого проєкту:** почни з **LocalExecutor + PostgreSQL**.
> Це повноцінний прод для одного сервера. Перейдеш на Celery/K8s, коли задач стане багато
> або знадобиться кілька машин.

---

<a name="7"></a>
## 7. Як писати DAG: 3 стилі + повний приклад на курсах валют

### 7.1 Три способи оголосити DAG

**Стиль 1 — класичний (context manager + Operators).** Найпоширеніший у легасі та докладних прикладах:

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

with DAG(dag_id="example", schedule="@daily", start_date=datetime(2024, 1, 1)) as dag:
    t1 = PythonOperator(task_id="extract", python_callable=my_extract)
    t2 = PythonOperator(task_id="load", python_callable=my_load)
    t1 >> t2   # залежність: спершу extract, потім load
```

**Стиль 2 — TaskFlow API (декоратори).** Сучасний, рекомендований для нового коду. Менше бойлерплейту,
XCom передається як звичайні аргументи функцій:

```python
from airflow.decorators import dag, task
from datetime import datetime

@dag(schedule="@daily", start_date=datetime(2024, 1, 1), catchup=False)
def example():
    @task
    def extract():
        return [{"r030": 840, "rate": 39.5}]

    @task
    def load(rows):
        print(len(rows))

    load(extract())   # залежність будується автоматично через передачу даних

example()
```

**Стиль 3 — кастомний Operator** (виносиш повторювану логіку в `plugins/operators/`). Про це в розділі 8.

### 7.2 Повний прод-приклад: DAG курсів валют НБУ

Це твій теперішній `run_fact_exchange_rate_etl`, розкладений на задачі Airflow.
Файл: `dags/exchange_rate_dag.py`.

```python
"""
DAG: щоденне завантаження курсів валют НБУ.
Запускається щодня о 09:00. Кожен DAG Run обробляє ОДНУ дату (logical_date),
тому backfill за минулі дати — це просто запуск ранів за ті дати.
"""
from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ── ТВІЙ бізнес-код. Лежить у include/etl/, Airflow його НЕ парсить як DAG ──
from etl.extract.fact_exchange_rate import extract_exchange_rates
from etl.transform.fact_exchange_rate import transform_exchange_rates
from etl.transform.dim_currency import transform_dim_currency
from etl.load.fact_exchange_rate import load_fact_exchange_rates
from etl.load.dim_currency import load_dim_currency

# default_args — параметри, спільні для всіх задач DAG.
# Аналогія: базовий клас/міксин, від якого "наслідуються" всі view.
default_args = {
    "owner": "data-team",
    "retries": 3,                              # 3 ретраї при падінні задачі
    "retry_delay": pendulum.duration(minutes=5),
    "retry_exponential_backoff": True,         # 5хв, 10хв, 20хв...
    "execution_timeout": pendulum.duration(minutes=15),
}


@dag(
    dag_id="exchange_rate_etl",
    description="Щоденні курси валют НБУ → PostgreSQL",
    schedule="0 9 * * *",                      # cron: щодня о 09:00
    start_date=pendulum.datetime(2024, 1, 1, tz="Europe/Kyiv"),
    catchup=False,                             # НЕ доганяти всі минулі дати автоматично
    max_active_runs=1,                         # не більше 1 запуску одночасно
    default_args=default_args,
    tags=["currency", "nbu", "etl"],           # для фільтрації в UI
)
def exchange_rate_etl():

    @task
    def extract(logical_date=None) -> list[dict]:
        """
        Витяг курсів за дату цього DAG Run.
        logical_date Airflow підставляє автоматично = дата, за яку йде run.
        Це і робить пайплайн ІДЕМПОТЕНТНИМ (див. розділ 9).
        """
        rate_date = logical_date.date()
        return extract_exchange_rates(rate_date=rate_date)   # твоя функція БЕЗ ЗМІН

    @task
    def load_currencies(raw: list[dict]) -> None:
        """Оновити довідник валют (dim_currency, upsert)."""
        df = transform_dim_currency(raw)
        hook = PostgresHook(postgres_conn_id="postgres_currency")
        conn = hook.get_conn()
        load_dim_currency(df, conn, mode="upsert")
        conn.commit()

    @task
    def load_rates(raw: list[dict]) -> dict:
        """Трансформувати й завантажити факти курсів (fact_exchange_rate, upsert)."""
        df = transform_exchange_rates(raw)
        hook = PostgresHook(postgres_conn_id="postgres_currency")
        conn = hook.get_conn()
        result = load_fact_exchange_rates(df, conn, mode="upsert")
        conn.commit()
        return result

    # ── Граф залежностей ──
    raw = extract()
    load_currencies(raw)        # dim спочатку (FK-цілісність)
    rates = load_rates(raw)

    # порядок: спершу довідник валют, потім факти
    raw >> load_currencies(raw) >> load_rates(raw)  # (спрощено; на практиці через chain())


exchange_rate_etl()
```

**Що тут важливо для розуміння:**

1. **Імпорти твого ETL — на верхньому рівні, але вони легкі** (просто `import`).
   Жодних `requests.get()` чи `pd.read_*` поза задачами — інакше Scheduler це виконуватиме при кожному парсингу!
2. **`logical_date`** — Airflow сам підставляє дату run-у. Це замінює твій ручний `iter_dates()`.
   Хочеш перезалити березень — запускаєш рани за дати березня, цикл не потрібен.
3. **`PostgresHook`** замість ручного `PostgreSQLConnector(host=..., password=...)` —
   креди беруться з Connection `postgres_currency`, не з коду.
4. **`retries` / `retry_delay`** — те, чого в `main.py` нема: автоматичні повтори лише впалої задачі,
   а не всього пайплайну.
5. **`catchup=False`** — захист від лавини backfill-ранів.

### 7.3 Той самий DAG, але з кастомним Operator (DRY)

Якщо в тебе багато схожих ETL (jsonplaceholder, currency, майбутні джерела) — повторювані три задачі
варто винести в один **кастомний Operator** (розділ 8). Тоді DAG стає таким:

```python
with DAG("exchange_rate_etl", schedule="0 9 * * *", ...) as dag:
    NBUExchangeRateOperator(
        task_id="load_rates",
        conn_id="postgres_currency",
    )
```

Уся логіка extract→transform→load схована в операторі. Це **прод-патерн для масштабування на багато джерел**.

---

<a name="8"></a>
## 8. Hooks, Operators, Sensors, XCom — словник

Це «цеглинки», з яких будуються DAG-и. Розуміти їх — значить розуміти Airflow.

### Operator — «що зробити»

Шаблон одиниці роботи. Готові оператори з провайдерів:

| Operator | Що робить |
|---|---|
| `PythonOperator` / `@task` | Виконати Python-функцію (99% твоїх задач) |
| `BashOperator` | Виконати shell-команду |
| `PostgresOperator` / `SQLExecuteQueryOperator` | Виконати SQL |
| `EmptyOperator` | Заглушка-вузол (групування, точки синхронізації) |
| `S3*`, `GCS*`, `Http*`... | Робота з хмарами/API (з provider-пакетів) |

> Аналогія: Operator ≈ Django Class-Based View. Готовий клас, який ти параметризуєш.

### Hook — «як підключитися до зовнішньої системи»

Hook інкапсулює підключення (БД, API, S3). Operator всередині використовує Hook.

```python
from airflow.providers.postgres.hooks.postgres import PostgresHook

hook = PostgresHook(postgres_conn_id="postgres_currency")
records = hook.get_records("SELECT * FROM fact_exchange_rate WHERE rate_date = %s", parameters=[d])
hook.insert_rows("dim_currency", rows)   # вже готові хелпери
```

> Аналогія: Hook ≈ Django database backend / API-клієнт. Твій `PostgreSQLConnector` — це фактично саморобний Hook.
> На проді його варто замінити на `PostgresHook` (або обгорнути у свій кастомний Hook у `plugins/hooks/`).

### Sensor — «зачекати, поки щось станеться»

Спеціальний Operator, що **чекає** умову: появу файлу, готовність таблиці, відповідь API.

```python
from airflow.providers.http.sensors.http import HttpSensor

wait_for_nbu = HttpSensor(
    task_id="wait_nbu_available",
    http_conn_id="nbu_api",
    endpoint="/NBUStatService/v1/statdirectory/exchange?json",
    poke_interval=60,         # перевіряти раз на хвилину
    timeout=60 * 30,          # здатися через 30 хв
    mode="reschedule",        # звільняти слот між перевірками (важливо для прод!)
)
wait_for_nbu >> extract_task
```

> Аналогія: «опитування» / polling. Прямого Django-аналога нема.
> Для курсів НБУ сенсор зазвичай не потрібен — API доступне постійно. Але корисно знати.

### XCom — «передати маленькі дані між задачами»

XCom (cross-communication) — спосіб передати **невеликий** результат з однієї задачі в іншу
(зберігається в metadata DB). У TaskFlow це відбувається автоматично, коли ти повертаєш значення з `@task`
і передаєш його в іншу.

```python
raw = extract()        # повернене значення → XCom
load_rates(raw)        # отримане з XCom як аргумент
```

> **Критично:** XCom — лише для **метаданих** (кілька рядків, числа, шляхи), НЕ для гігабайтів даних!
> Великі DataFrame не ганяють через XCom. На проді патерн такий: extract пише дані у файл (S3/диск),
> а через XCom передає **шлях** до файлу. Або всі три кроки роблять в одній задачі (як у прикладі 7.2,
> де `raw` — це список dict помірного розміру, що нормально).

### Кастомний Operator (приклад для `plugins/operators/`)

```python
# plugins/operators/nbu_exchange_operator.py
from airflow.models.baseoperator import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from etl.extract.fact_exchange_rate import extract_exchange_rates
from etl.transform.fact_exchange_rate import transform_exchange_rates
from etl.load.fact_exchange_rate import load_fact_exchange_rates


class NBUExchangeRateOperator(BaseOperator):
    """Повний E-T-L курсів НБУ за дату run-у в одному операторі."""

    def __init__(self, conn_id: str, **kwargs):
        super().__init__(**kwargs)
        self.conn_id = conn_id

    def execute(self, context):
        # context["logical_date"] — дата поточного DAG Run
        rate_date = context["logical_date"].date()

        raw = extract_exchange_rates(rate_date=rate_date)
        df = transform_exchange_rates(raw)

        conn = PostgresHook(postgres_conn_id=self.conn_id).get_conn()
        result = load_fact_exchange_rates(df, conn, mode="upsert")
        conn.commit()

        self.log.info("Loaded %s rows for %s", result["rows"], rate_date)
        return result   # потрапить у XCom
```

> Аналогія: винесення повторюваної логіки в reusable CBV/mixin у Django. Пишеш раз — використовуєш у багатьох DAG.

---

<a name="9"></a>
## 9. Backfill, catchup, idempotency — головне для ETL

Це те, заради чого ETL-інженери і люблять Airflow. Розберись детально.

### logical_date (раніше execution_date)

Кожен DAG Run має **`logical_date`** — дату/час *періоду*, за який він відповідає.
Для `@daily` DAG, що запустився вранці 6 березня, `logical_date` = 5 березня (кінець попереднього періоду).
Це і є та дата, за яку треба тягнути дані.

> Це **замінює твій `iter_dates(start_date, end_date)`**. Замість циклу в коді — Airflow створює окремий
> Run на кожну дату, кожен зі своїм `logical_date`.

### catchup — догнати минуле

```python
@dag(start_date=pendulum.datetime(2024, 1, 1), schedule="@daily", catchup=True)
```

Якщо `catchup=True`, а сьогодні червень 2026 — Airflow при увімкненні DAG **створить рани за КОЖЕН день**
з 1 січня 2024. Це і є автоматичний **backfill**.

> **На проді майже завжди ставлять `catchup=False`** і роблять backfill вручну й контрольовано (див. нижче),
> щоб не отримати раптом 800 одночасних ранів і не «задідосити» API НБУ.

### Backfill вручну — «перезалий березень»

```bash
# Запустити рани за діапазон дат
airflow dags backfill exchange_rate_etl \
    --start-date 2024-03-01 \
    --end-date 2024-03-31
```

Або в UI: вибрати рани → Clear → вони перезапустяться. Жодних правок коду.

### Idempotency — НАЙВАЖЛИВІШЕ правило ETL

**Ідемпотентність** = повторний запуск за ту саму дату дає той самий результат, без дублів.

Твій код **уже частково ідемпотентний** завдяки `mode="upsert"` у `load_fact_exchange_rates` —
це правильно! Прод-правило формулюється так:

> Кожен Task має оперувати **лише даними свого `logical_date`** і використовувати **UPSERT / DELETE-INSERT**,
> щоб повторний запуск не плодив дублі.

Антипатерн (НЕ роби так): `INSERT` без перевірки + «взяти все за сьогодні `date.today()`» всередині задачі.
Тоді backfill зламається (бо `date.today()` завжди сьогодні) і ретрай створить дублі.

Правильно (як у твоєму прикладі DAG): дата приходить з `logical_date`, запис іде через `upsert`.

```
catchup=False   ─►  не доганяти автоматично
logical_date    ─►  кожен run знає "свою" дату (замість iter_dates)
upsert/merge    ─►  ретрай і backfill безпечні (без дублів)
```

---

<a name="10"></a>
## 10. Production deployment (Docker / K8s), CI/CD

### 10.1 Docker Compose (старт прод-стеку)

Офіційний спосіб — взяти `docker-compose.yaml` з документації Airflow і додати свої залежності.
Структура сервісів:

```yaml
# спрощено — головне зрозуміти склад
services:
  postgres:       # Metadata DB (НЕ твоя ETL-база!)
    image: postgres:16
  redis:          # брокер для CeleryExecutor
    image: redis:7
  airflow-webserver:
    image: my-airflow:latest      # твій кастомний образ
    command: webserver
    ports: ["8080:8080"]
  airflow-scheduler:
    image: my-airflow:latest
    command: scheduler
  airflow-worker:
    image: my-airflow:latest
    command: celery worker
  airflow-init:                    # одноразово: db migrate + створення адміна
    image: my-airflow:latest
    command: version
```

Свій `Dockerfile` (щоб були `pandas`, `requests`, `psycopg2` і твій `etl/`):

```dockerfile
FROM apache/airflow:2.10.0-python3.12

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

# твій бізнес-код як importable-пакет
COPY include/etl /opt/airflow/etl
ENV PYTHONPATH="/opt/airflow:${PYTHONPATH}"
```

> Аналогія першого запуску з Django:
> `airflow db migrate` ≈ `manage.py migrate`,
> `airflow users create ...` ≈ `createsuperuser`,
> `airflow webserver` ≈ `runserver`.

### 10.2 Kubernetes (великий прод)

Стандарт — **офіційний Helm-чарт Airflow** або **Astronomer**. DAG-и доставляють трьома способами:
(1) **git-sync** sidecar (контейнер тягне репозиторій із DAG-ами кожні N сек),
(2) запікання DAG-ів в образ,
(3) спільний том (PVC).
git-sync — найпопулярніший: запушив у git → DAG з'явився, без ребілду образу.

### 10.3 CI/CD

Типовий pipeline (GitHub Actions / GitLab CI):

```
push → lint (ruff/black) → DAG integrity test (всі DAG парсяться?) →
unit-тести ETL → build Docker image → deploy (git-sync підхопить dags/)
```

Обов'язковий тест — **DAG integrity** (ловить помилки до прода):

```python
# tests/test_dag_integrity.py
from airflow.models import DagBag

def test_no_import_errors():
    dag_bag = DagBag(dag_folder="dags/", include_examples=False)
    assert not dag_bag.import_errors, dag_bag.import_errors
```

> Аналогія: як Django `manage.py check` + тести перед деплоєм.

---

<a name="11"></a>
## 11. Моніторинг, логи, алерти, безпека

### Логи

- Кожен Task Instance має **свій лог**, видимий у UI (клік на задачу → Logs).
- На проді локальні логи воркера ефемерні → вмикають **remote logging** у S3/GCS/Elasticsearch
  (`AIRFLOW__LOGGING__REMOTE_LOGGING=True`).
- Твій теперішній `logging` всередині `extract/transform/load` **продовжує працювати** —
  Airflow перехоплює stdout/stderr задачі в її лог. Файловий `LOG_FILE='etl.log'' на проді прибирають.

### Алерти

```python
default_args = {
    "email": ["data-team@company.com"],
    "email_on_failure": True,
    "email_on_retry": False,
}
```

Сучасніше — callbacks (Slack/Telegram при падінні):

```python
def slack_on_fail(context):
    ti = context["task_instance"]
    send_slack(f"❌ {ti.dag_id}.{ti.task_id} failed at {context['logical_date']}")

@dag(..., on_failure_callback=slack_on_fail)
def exchange_rate_etl(): ...
```

Також на проді ставлять **SLA** (задача має завершитись за X) і метрики через **StatsD/Prometheus** → Grafana.

### Безпека

- **Креди — лише в Connections/Variables або secret backend** (HashiCorp Vault, AWS Secrets Manager),
  ніколи не в коді DAG. Це прямо лікує твій теперішній хардкод дефолтів у `config.py`.
- **RBAC** у вебсервері: ролі Admin / Op / User / Viewer.
- Webserver — лише за HTTPS і за VPN/SSO.

---

<a name="12"></a>
## 12. Покроковий план міграції твого проєкту

Конкретні кроки під `etl_project`. Робочий код **не переписуєш** — лише переносиш і додаєш шар DAG.

**Крок 1. Реорганізувати репозиторій.**
```
src/  ──►  include/etl/        (extract / transform / load / connectors — як є)
+ dags/                        (новий шар)
+ plugins/                     (кастомні Operators/Hooks — пізніше)
+ Dockerfile, docker-compose.yaml, requirements.txt (доповнити airflow-провайдерами)
```
Правиш лише імпорти: `from src.extract...` → `from etl.extract...`.

**Крок 2. Підняти Airflow локально** (офіційний docker-compose, LocalExecutor для старту).
`airflow db migrate` → `airflow users create` → відкрити UI на `:8080`.

**Крок 3. Завести Connection `postgres_currency`** (ті самі креди, що в `config.DATABASES`)
через UI або env `AIRFLOW_CONN_POSTGRES_CURRENCY=...`. Прибрати креди з коду.

**Крок 4. Написати перший DAG** `dags/exchange_rate_dag.py` (готовий приклад у розділі 7.2).
Замінити `PostgreSQLConnector(host=...)` на `PostgresHook(postgres_conn_id="postgres_currency")`.

**Крок 5. Прибрати оркестрацію з коду:**
- `iter_dates()` → не потрібен, дату дає `logical_date`;
- `main.run_all()` → стає набором окремих DAG-ів (`exchange_rate_dag.py`, `jsonplaceholder_dag.py`);
- ретраї в `extract` (твій `for attempt in range(retries)`) можна лишити, але краще довірити Airflow (`retries=3`).

**Крок 6. Зробити пайплайн ідемпотентним** (у тебе вже є `mode="upsert"` — добре). Перевірити, що
жодна задача не використовує `date.today()` всередині, а лише `logical_date`.

**Крок 7. Додати DAG-integrity тест** у CI (розділ 10.3) і алерти на падіння (розділ 11).

**Крок 8. Backfill історії** контрольовано:
`airflow dags backfill exchange_rate_etl --start-date 2024-01-01 --end-date 2024-12-31`
(замість теперішнього хардкоду дат у `main.py`).

---

## Резюме одним абзацом

Airflow — це **оркестратор**: він не замінює твій ETL-код, а **запускає його за розкладом, з ретраями,
історією та можливістю перезаливати окремі дати**. Твій `src/` стає `include/etl/` майже без змін;
зверху ти додаєш тонкий шар `dags/`, де описуєш граф `extract >> transform >> load`. Підключення переїжджають
з `config.py` у **Connections**, дати — з `iter_dates()` у **`logical_date`**, а кнопка «запустити вручну»
з `main.py` — у **UI зі Scheduler-ом**. Головні прод-принципи: легкий код на рівні DAG-файлу, важка робота —
лише в задачах; `catchup=False` + контрольований backfill; **ідемпотентність через upsert**; креди — в Connections,
не в коді.
```

