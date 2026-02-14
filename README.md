# ClickHouse Incremental ETL (PySpark)

## Overview
A simple PySpark ETL pipeline that incrementally extracts data from a PostgreSQL table and loads it into a ClickHouse table.

This project is built as a ClickHouse hands-on exercise, demonstrating:

- Incremental data extraction 
- JDBC integration with Spark
- Dockerized local environment

## 🧰 Tech Stack
- Python
- PySpark
- PostgreSQL
- ClickHouse
- Docker & Docker Compose
- JDBC Drivers:
    - PostgreSQL JDBC
    - ClickHouse JDBC

## 🔁 Incremental Strategy
The ETL uses a simple watermark pattern:
- Watermark column: updated_at
- Logic: WHERE updated_at > max(updated_at in ClickHouse)

## Quickstart
1. Start docker services
```bash
docker-compose up -d
```
2. Create **data_source** schema in postgres.
3. Create two tables in postgres and clickkhouse using the `ddl.sql` files
4. Insert data in postgres using `insert_records.sql`
5. Run the spark application:
```bash
python3 spark/src/main.py
```
6. Check loaded data in clickhouse table
```sql
SELECT * FROM `default`.app_user_visits_fact FINAL;  --FINAL to merge and view latest records
SELECT count(*) FROM `default`.app_user_visits_fact FINAL;
```