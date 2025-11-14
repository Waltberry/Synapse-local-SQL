# Synapse-Style SQL on Files (DuckDB & Spark) — Dockerized

Practice **querying CSV/JSON/Parquet directly from a lake-style folder layout** using SQL, entirely **locally**.
Two engines share the same `data/` folder:

* **DuckDB** (Python runner) — runs SQL in `duckdb/sql`
* **Spark SQL** (PySpark runner) — runs SQL in `spark/sql`
* **Pipelines (HTTP → files)** — tiny “Copy Data” analogs to fetch sample data:

  * `pipelines/copy_products.py` → `data/product_data/products.csv`
  * `pipelines/copy_retail.py`   → `data/lake/RetailDB/{Customer,Product,SalesOrder}/*.csv`

This mirrors Synapse *serverless SQL* patterns: **query files in place**, **write partitioned Parquet**, **prune partitions**, **CETAS-style transforms**, and a **“lake database” analog** (views/tables over folders).

---

## Features

* Read **CSV** (auto-infer or explicit schema), **JSON Lines (NDJSON)**, **Parquet**
* Write **partitioned Parquet** by `year` derived from `OrderDate`
* **CETAS-style transforms**

  * DuckDB: `COPY (SELECT …) TO 'data/cetas/...'(FORMAT PARQUET …)`
  * Spark:  `INSERT OVERWRITE DIRECTORY … USING PARQUET` or `CTAS … USING PARQUET LOCATION …`
* **Lake DB analog**: declare views/tables over `data/lake/RetailDB/...` and join them
* Reproducible SQL for both engines; tabular outputs land in `outputs/`

---

## Prerequisites

* **Docker Desktop** (Windows/macOS) or Docker Engine (Linux)
* **Docker Compose v2+** (`docker compose …`)
* Windows: ensure Docker Desktop is running

---

## Quick Start

```bash
# from repo root

# 0) (Optional) Clean previous CETAS outputs
# rm -rf data/cetas/productsales data/cetas/yearlysales

# 1) Ingest sample datasets
docker compose run --rm pipeline python pipelines/copy_products.py
docker compose run --rm pipeline python pipelines/copy_retail.py   # populates data/lake/RetailDB/...

# 2) DuckDB flow: CSV/JSON → Parquet partitions → SQL → outputs/
docker compose run --rm duckdb

# 3) Spark SQL flow: same idea in PySpark → outputs/
docker compose run --rm spark
```

* Results: `outputs/`
* Parquet partitions: `data/parquet/orders/year=.../`
* CETAS outputs: `data/cetas/.../`
* Lake-DB sample data: `data/lake/RetailDB/...`

---

## Project Structure

```
synapse-local-sql/
├─ docker-compose.yml
├─ data/
│  ├─ csv/                            # sales_2019/2020/2021.csv
│  ├─ json/                           # orders.jsonl (NDJSON)
│  ├─ lake/
│  │  └─ RetailDB/
│  │     ├─ Customer/customer.csv     # headerless CSVs (pipelines/copy_retail.py)
│  │     ├─ Product/product.csv
│  │     └─ SalesOrder/salesorder.csv
│  ├─ parquet/                        # generated Parquet (partitioned by year)
│  ├─ cetas/                          # CETAS-style outputs (productsales/yearlysales)
│  └─ product_data/                   # products.csv (pipelines/copy_products.py)
├─ pipelines/
│  ├─ copy_products.py                # HTTP → file
│  └─ copy_retail.py                  # HTTP → lake/RetailDB/...
├─ duckdb/
│  ├─ scripts/run_all.py
│  └─ sql/
│     ├─ 01_csv.sql
│     ├─ 02_parquet.sql
│     ├─ 03_json.sql
│     ├─ 04_cetas.sql
│     ├─ 10_products_top100.sql
│     ├─ 11_products_counts.sql
│     ├─ 20_mini_warehouse.sql
│     ├─ 50_retail_schema.sql         # views over RetailDB folders (explicit column list)
│     └─ 51_retail_join.sql
├─ spark/
│  ├─ scripts/bootstrap_and_run.py
│  └─ sql/
│     ├─ 01_csv.sql
│     ├─ 02_parquet.sql
│     ├─ 03_json.sql
│     ├─ 04_cetas.sql
│     ├─ 10_products_counts.sql
│     ├─ 50_retail_schema.sql         # tables over RetailDB folders (schema inference)
│     ├─ 51_retail_join.sql
│     └─ 52_retail_insert.sql         # demo INSERT into SalesOrder
└─ outputs/
```

---

## How It Works

1. **Pipelines** fetch sample CSVs (products + retail “lake DB”).
2. **Serverless-style SQL over files**

   * DuckDB: `read_csv_auto / read_parquet / read_json_auto`
   * Spark:  `spark.read.csv / json / parquet`
3. **Augment** with `year = YEAR(OrderDate)`
4. **Write Parquet** partitioned by `year` → `data/parquet/orders/year=…/`
5. **Run SQL scripts** (aggregations, filters, joins) → `outputs/`
6. **CETAS-style** persist transformed results under `data/cetas/...`
7. **Lake DB analog**

   * DuckDB: views over headerless CSVs (`50_retail_schema.sql`) + join (`51_retail_join.sql`)
   * Spark: external tables over folders (`50_retail_schema.sql`) + join + insert (`51/52`)

---

## What to Run (maps to Synapse labs)

### A) “Serverless SQL” over **products.csv**

* DuckDB: `10_products_top100.sql`, `11_products_counts.sql`
* Spark:  `10_products_counts.sql`

### B) CSV → **Parquet partitions** → prune by `year`

* DuckDB: `01_csv.sql`, `02_parquet.sql`
* Spark:  `01_csv.sql`, `02_parquet.sql`

### C) **JSON Lines** query (OPENROWSET JSON analog)

* DuckDB: `03_json.sql`
* Spark:  `03_json.sql`

### D) **CETAS-style transforms** (persist transformed results)

* DuckDB: `04_cetas.sql`

  * Writes → `data/cetas/productsales/*.parquet`,
    `data/cetas/yearlysales/CalendarYear=.../*.parquet`
* Spark:  `04_cetas.sql`

  * `INSERT OVERWRITE DIRECTORY` → `data/cetas/productsales/`
  * `CTAS … LOCATION '/workspace/data/cetas/yearlysales'`

### E) **Lake Database analog** (RetailDB over folders)

* **Prepare data**: `docker compose run --rm pipeline python pipelines/copy_retail.py`
* **DuckDB**: `50_retail_schema.sql` → views, then `51_retail_join.sql`
* **Spark**:  `50_retail_schema.sql` → tables, `51_retail_join.sql` (join), `52_retail_insert.sql` (insert demo)

> RetailDB CSVs are **headerless**. DuckDB script defines a column list; Spark uses schema inference but you can `USING csv OPTIONS(header 'false')` if you prefer explicitness.

---

## Outputs

* **DuckDB**: one CSV per script in `outputs/duckdb_*.csv`
* **Spark**: folder per script in `outputs/spark_*.csv_dir/part-*.csv`
* **CETAS**: Parquet in `data/cetas/...` (primary artifact)

---

## Troubleshooting

* **CETAS re-runs**: delete existing target folders then re-run:

  ```bash
  rm -rf data/cetas/productsales data/cetas/yearlysales
  ```
* **Spark CTAS to non-empty LOCATION**: either delete the folder or:

  ```sql
  SET spark.sql.legacy.allowNonEmptyLocationInCTAS = true;
  ```
* **Spark “Datasource does not support writing empty schema” WARNs**:
  benign when your runner prints DDL/INSERT statements (no tabular output).
* **RetailDB (DuckDB) missing columns**: the CSVs are headerless—keep the explicit
  column list in `50_retail_schema.sql`.
* **Compose `version` warning**: remove `version: "3.9"` from `docker-compose.yml`.

---

## Cleanup

Stop/remove containers & networks:

```bash
docker compose down
```

Free disk (dangling images, stopped containers, unused networks/volumes):

```bash
docker system prune -a
```
