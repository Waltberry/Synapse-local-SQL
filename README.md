# Synapse-Style SQL on Files (DuckDB & Spark) — Dockerized

Practice **querying CSV/JSON/Parquet directly from a data lake layout** using SQL, locally.
Two engines share the same `data/` folder:

* **DuckDB** (Python runner) — executes SQL in `duckdb/sql`
* **Spark SQL** (PySpark runner) — executes SQL in `spark/sql`

This mirrors Synapse “serverless SQL” patterns: **query files in place**, **partition with Parquet**, and **pay attention to partition pruning**.

---

## Features

* Read **CSV** (schema inference or explicit), **JSON Lines**, **Parquet**
* Write **partitioned Parquet** by `year` derived from `OrderDate`
* Run reproducible SQL scripts for both engines
* Save results to `outputs/` (CSV)

---

## Prerequisites

* **Docker Desktop** (Windows/macOS) or Docker Engine (Linux)
* **Docker Compose** (v2+). Use `docker compose ...` (or `docker-compose ...` if you’re on v1)

> Windows: ensure Docker Desktop is running.

---

## Quick Start

```bash
# from repo root
# DuckDB flow
docker compose run --rm duckdb

# Spark SQL flow
docker compose run --rm spark
```

Results appear in `outputs/`. Parquet is written to `data/parquet/orders/year=.../`.

> You may see a warning that the `version` key in `docker-compose.yml` is obsolete; it’s harmless.
> To silence it, remove the first line `version: "3.9"`.

---

## Project Structure

```
synapse-local-sql/
├─ docker-compose.yml
├─ data/
│  ├─ csv/                  # sample sales_2019/2020/2021.csv
│  └─ json/                 # orders.jsonl (NDJSON)
├─ duckdb/
│  ├─ scripts/run_all.py    # reads CSV/JSON, writes Parquet, runs SQL files
│  └─ sql/
│     ├─ 01_csv.sql
│     ├─ 02_parquet.sql
│     └─ 03_json.sql
├─ spark/
│  ├─ scripts/bootstrap_and_run.py  # same flow in PySpark
│  └─ sql/
│     ├─ 01_csv.sql
│     ├─ 02_parquet.sql
│     └─ 03_json.sql
└─ outputs/                # query results (generated)
```

---

## How It Works

1. **Read CSV** from `data/csv/*.csv` (headers, inferred types).
2. **Derive `year`** from `OrderDate`.
3. **Write Parquet** partitioned by `year` to `data/parquet/orders/`.
4. **Read JSON Lines** from `data/json/orders.jsonl`.
5. **Run SQL** (aggregations, partition filters) and write results to `outputs/`.

**Flow sketch:**

```
        data/csv/*.csv         data/json/*.jsonl
             │                         │
             ├───────────────┬─────────┤
             │               │
        DuckDB runner     Spark runner
     (duckdb/scripts)   (spark/scripts)
             │               │
      read_csv_auto()     spark.read.csv/json()
          + JSON             + JSON
             │               │
     + derive year        + derive year
     + write Parquet      + write Parquet
   (partitionBy year)   (partitionBy year)
             │               │
   read_parquet(...)   read.parquet(...)
   + run SQL files      + run SQL files
             │               │
          outputs/        outputs/
```

---

## Using Your Own Data

* Drop additional CSVs into `data/csv/` (must include `OrderDate` column).
* Add more JSON Lines to `data/json/orders.jsonl`.
* Re-run `duckdb` or `spark` services; new partitions (e.g., `year=2022/`) will be created automatically.

---

## Outputs

* **DuckDB**: single CSV per script, e.g., `outputs/duckdb_01_csv.csv`
* **Spark**: folder per script (e.g., `outputs/spark_01_csv.csv_dir/part-*.csv`)

  * You can post-process to a single file if desired.

---

## Troubleshooting

* **Docker Desktop not running (Windows):** start Docker Desktop, then re-run.
* **Obsolete `version` warning:** remove `version: "3.9"` from `docker-compose.yml`.
* **Spark Java runtime:** this repo installs **OpenJDK 21** in the Spark container.
  If you prefer Java 17, switch the Spark service image to `eclipse-temurin:17-jre` and install Python inside the container.
* **No Parquet directory error (DuckDB):** ensure the script creates `data/parquet/orders/` (already handled) or create it manually once.

---

## Why This Mirrors Synapse Serverless

* **No database ingest**: query files directly with SQL.
* **Columnar Parquet + partitions**: typical lakehouse layout.
* **Partition pruning**: filter by `year` to scan less and go faster.
* **Two engines**: confirm logic is engine-agnostic (DuckDB & Spark).