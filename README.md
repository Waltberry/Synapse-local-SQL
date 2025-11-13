# Synapse-Style SQL on Files (DuckDB & Spark) — Dockerized

Practice **querying CSV/JSON/Parquet directly from a lake-style folder layout** using SQL, entirely **locally**.
Two engines share the same `data/` folder:

* **DuckDB** (Python runner) — executes SQL in `duckdb/sql`
* **Spark SQL** (PySpark runner) — executes SQL in `spark/sql`
* **Pipeline (HTTP → file)** — simple “Copy Data” analog to fetch sample data

This mirrors Synapse “serverless SQL” patterns: **query files in place**, **write partitioned Parquet**, and **use partition pruning**.

---

## Features

* Read **CSV** (auto-infer or explicit schema), **JSON Lines (NDJSON)**, **Parquet**
* Write **partitioned Parquet** by `year` derived from `OrderDate`
* Simple **ingest pipeline** (HTTP → `data/product_data/products.csv`)
* Reproducible SQL for both engines; results saved to `outputs/` as CSV

---

## Prerequisites

* **Docker Desktop** (Windows/macOS) or Docker Engine (Linux)
* **Docker Compose v2+** (`docker compose …`)
* Windows users: ensure Docker Desktop is running

> You may see a warning that the `version` key in `docker-compose.yml` is obsolete; it’s harmless.
> To silence it, remove the line `version: "3.9"`.

---

## Quick Start

```bash
# from repo root

# 1) Copy Data (HTTP → file): fetch products.csv into data/product_data/
docker compose run --rm pipeline

# 2) DuckDB flow: CSV/JSON → Parquet partitions → run SQL → outputs/
docker compose run --rm duckdb

# 3) Spark SQL flow: same idea in PySpark → outputs/
docker compose run --rm spark
```

Results appear in `outputs/`. Parquet is written to `data/parquet/orders/year=.../`.

---

## Project Structure

```
synapse-local-sql/
├─ docker-compose.yml
├─ data/
│  ├─ csv/                           # sample sales_2019/2020/2021.csv
│  ├─ json/                          # orders.jsonl (NDJSON)
│  ├─ parquet/                       # generated Parquet (partitioned by year)
│  └─ product_data/                  # products.csv (downloaded by pipeline)
├─ pipelines/
│  └─ copy_products.py               # HTTP → file (Copy Data analog)
├─ duckdb/
│  ├─ scripts/run_all.py             # reads CSV/JSON, writes Parquet, runs SQL
│  └─ sql/
│     ├─ 01_csv.sql
│     ├─ 02_parquet.sql
│     ├─ 03_json.sql
│     ├─ 10_products_top100.sql      # OPENROWSET-style "TOP 100"
│     ├─ 11_products_counts.sql      # category counts
│     └─ 20_mini_warehouse.sql       # Dim/Fact joins (Dedicated SQL analog)
├─ spark/
│  ├─ scripts/bootstrap_and_run.py    # same flow in PySpark
│  └─ sql/
│     ├─ 01_csv.sql
│     ├─ 02_parquet.sql
│     ├─ 03_json.sql
│     └─ 10_products_counts.sql
└─ outputs/                           # query results (generated)
```

---

## How It Works

1. **Ingest (pipeline)** — Download `products.csv` to `data/product_data/`.
2. **Serverless-style SQL over files**

   * **DuckDB**: `read_csv_auto/read_parquet/read_json_auto`
   * **Spark**: `spark.read.csv/json/parquet`
3. **Augment** with `year = YEAR(OrderDate)`
4. **Write Parquet** partitioned by `year` → `data/parquet/orders/year=…/`
5. **Run SQL scripts** (aggregations, filters, joins) → `outputs/`

**Flow sketch:**

```
        data/csv/*.csv         data/json/*.jsonl         (pipeline) products.csv
             │                         │                         │
             ├───────────────┬─────────┤                         │
             │               │                                       ↓
        DuckDB runner     Spark runner                     data/product_data/products.csv
     (duckdb/scripts)   (spark/scripts)
             │               │
      read_csv/json()     spark.read.csv/json()
             │               │
       + derive year       + derive year
       + write Parquet     + write Parquet
     (partitionBy year)  (partitionBy year)
             │               │
   read_parquet(...)   read.parquet(...)
   + run SQL files      + run SQL files
             │               │
          outputs/        outputs/
```

---

## What to Run (mirrors the Synapse lab)

### A) “Serverless SQL” over **products.csv**

* DuckDB: `duckdb/sql/10_products_top100.sql`, `11_products_counts.sql`
* Spark: `spark/sql/10_products_counts.sql`

### B) CSV → **Parquet partitions** → prune by `year`

* DuckDB: `duckdb/sql/01_csv.sql`, `02_parquet.sql`
* Spark:  `spark/sql/01_csv.sql`,  `02_parquet.sql`

### C) **JSON Lines** query (OPENROWSET JSON analog)

* DuckDB: `duckdb/sql/03_json.sql`
* Spark:  `spark/sql/03_json.sql`

### D) Mini **Dedicated SQL pool** (warehouse-style joins)

* DuckDB: `duckdb/sql/20_mini_warehouse.sql` (builds `wh.Dim*` + `wh.Fact*`, runs join)

---

## Using Your Own Data

* Drop additional CSVs into `data/csv/` (**must include `OrderDate`**).
* Append NDJSON to `data/json/orders.jsonl`.
* Re-run `duckdb` or `spark`; new Parquet partitions (e.g., `year=2022/`) appear automatically.

---

## Outputs

* **DuckDB**: one CSV per script, e.g. `outputs/duckdb_01_csv.csv`
* **Spark**: a folder per script, e.g. `outputs/spark_01_csv.csv_dir/part-*.csv`

  * (You can post-process to a single file if you prefer.)

---

## Troubleshooting

* **Docker Desktop not running (Windows):** start it, then re-run the command.
* **Compose “version” warning:** remove `version: "3.9"` from `docker-compose.yml`.
* **Spark Java runtime:** Spark service installs **OpenJDK 21** in-container.
  Prefer Java 17? Use an `eclipse-temurin:17-jre` image and install Python in the container.
* **DuckDB “No such file or directory” when writing Parquet:** the script now creates `data/parquet/orders/`. If you changed paths, create the folder or adjust the script.

---

## Why This Mirrors Synapse Serverless

* **No DB ingest** — query files directly with SQL
* **Columnar Parquet + partitions** — standard lakehouse layout
* **Partition pruning** — filter by `year` for less scan & faster queries
* **Two engines** — prove queries are engine-agnostic (DuckDB & Spark)
