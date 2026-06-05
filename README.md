# ETL Pipeline with PostgreSQL

Production-ready ETL pipeline that extracts data from APIs, transforms it, and loads into PostgreSQL.

## Architecture

```
Extract (API) → Transform (Clean, Validate) → Load (PostgreSQL)
```

### Features

- **Extract**: Fetch data from external APIs with retry logic
- **Transform**: Clean, validate, and transform data with Pandas
- **Load**: Batch insert/upsert into PostgreSQL
- **Logging**: Detailed logs to both file and console
- **Docker**: PostgreSQL runs in Docker with persistent volumes
- **Type Safety**: Full type hints throughout the code

## Quick Start

### 1. Prerequisites

- Docker & Docker Compose installed
- Python 3.8+

### 2. Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Start PostgreSQL in Docker
docker-compose up -d

# Wait for PostgreSQL to be ready (healthcheck)
docker-compose ps
```

### 3. Run ETL

```bash
python main.py
```

### 4. Verify Data

```bash
# Connect to PostgreSQL
docker-compose exec postgres psql -U etl_user -d etl_db

# View tables
\dt
SELECT COUNT(*) FROM users;
SELECT COUNT(*) FROM posts;
SELECT COUNT(*) FROM comments;

# Exit
\q
```

## Configuration

Edit `.env` to change database parameters:

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=etl_db
DB_USER=etl_user
DB_PASSWORD=etl_password
```

## Project Structure

```
etl_project/
├── src/
│   ├── extract/          # Data extraction
│   ├── transform/        # Data transformation
│   ├── load/             # Data loading
│   ├── loaders/          # Loader implementations
│   └── connectors/       # Database connectors
├── docker-compose.yml    # PostgreSQL setup
├── schema.sql            # Database schema
├── config.py             # Configuration
├── main.py               # Entry point
├── requirements.txt      # Dependencies
└── .env                  # Environment variables
```

## How It Works

### Extract Phase
- Fetches data from JSONPlaceholder API
- Includes retry logic (3 attempts with backoff)
- Handles timeouts and connection errors

### Transform Phase
- Converts JSON to Pandas DataFrame
- Selects and renames columns
- Cleans: removes NULL values, duplicates, whitespace
- Validates: checks email format, data ranges, etc.
- Type casting: converts to proper data types

### Load Phase
- Batch processing (100 rows per batch)
- Supports three modes:
  - `insert`: Add new rows only
  - `upsert`: Update existing, add new (by primary key)
  - `replace`: Delete all and reload
- Full transaction support

## Docker

### Start Services

```bash
docker-compose up -d
```

### View Logs

```bash
docker-compose logs -f postgres
```

### Stop Services

```bash
docker-compose down
```

### Persistent Data

Data is stored in Docker volume `postgres_data` and persists even after stopping containers.

### Connect to Database

```bash
# Using psql
docker-compose exec postgres psql -U etl_user -d etl_db

# Or with psycopg2 in Python
from src.connectors.postgresql import PostgreSQLConnector
conn = PostgreSQLConnector('localhost', 5432, 'etl_db', 'etl_user', 'etl_password')
conn.connect()
```

## Database Schema

### Users Table
```sql
user_id (PK)
name
username (UNIQUE)
email (UNIQUE)
city
company_name
created_at
updated_at
```

### Posts Table
```sql
post_id (PK)
user_id (FK → users)
title
body
created_at
updated_at
```

### Comments Table
```sql
comment_id (PK)
post_id (FK → posts)
name
email
body
created_at
updated_at
```

## Running with Different Databases

The architecture supports multiple database types. To add a new database:

1. Create `src/loaders/new_db_loader.py` implementing `BaseLoader`
2. Create `src/connectors/new_db.py` with connection logic
3. Update `load/` functions to use new loader
4. Update `config.py` and `.env` for new database parameters

## Logging

Logs are written to both console and `etl.log` file:

```
2026-06-02 16:32:31,931 - src.extract.users - INFO - [OK] Extracted 10 users
2026-06-02 16:32:31,931 - src.transform.users - INFO - [OK] Transformed 10 users
2026-06-02 16:32:31,931 - src.load.users - INFO - [OK] Loaded 10 users
```

## Performance Notes

- Batch size: 100 rows (configurable in `.env`)
- Retry attempts: 3 (configurable in extractors)
- PostgreSQL indexes on foreign keys for fast joins

## Future Improvements

- [ ] Apache Airflow integration for scheduling, orchestration, monitoring, retries, and backfills
- [ ] Parallel extraction for multiple tables
- [ ] Data quality checks and validation rules
- [ ] Incremental loading (only new data)
- [ ] Error recovery and deadletter queue
- [ ] Performance monitoring and metrics
