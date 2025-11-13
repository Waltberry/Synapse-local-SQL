# Synapse-style local SQL over files (DuckDB + Spark), dockerized

Practice querying **CSV / JSON / Parquet** like Synapse serverless SQL — but **locally**.
Two services share the same `/data` folder:
- **DuckDB** (Python runner) executes SQL files in `duckdb/sql`
- **Spark SQL** (PySpark runner) executes SQL files in `spark/sql`

## Prereqs
- Docker + Docker Compose

## Quickstart
```bash
cd synapse-local-sql
# Run DuckDB flow
docker compose run --rm duckdb

# Run Spark flow
docker compose run --rm spark
```
Results land in `outputs/` (CSVs). The containers also **create Parquet** from CSV, partitioned by `year`.

## Project layout
```
synapse-local-sql/
├─ docker-compose.yml
├─ data/
│  ├─ csv/    # sample sales_2019/2020/2021.csv
│  └─ json/   # orders.jsonl (NDJSON)
├─ duckdb/
│  ├─ scripts/run_all.py     # reads CSV/JSON, writes Parquet, runs SQL
│  └─ sql/
│     ├─ 01_csv.sql
│     ├─ 02_parquet.sql
│     └─ 03_json.sql
├─ spark/
│  ├─ scripts/bootstrap_and_run.py  # same flow with PySpark
│  └─ sql/
│     ├─ 01_csv.sql
│     ├─ 02_parquet.sql
│     └─ 03_json.sql
└─ outputs/   # query results (created at runtime)
```

## Flow (both engines)
1. **Read CSV** (`data/csv/*.csv`) with header + infer schema
2. **Augment** with `year` from `OrderDate`, **write Parquet partitioned by year**
3. **Read JSON Lines** (`data/json/orders.jsonl`)
4. **Run SQL** (aggregations, partition filters) and write outputs

## Simple flow diagram
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

## Notes
- You can drop more files into `data/csv` or `data/json` and re-run.
- Swap in your own datasets; both engines automatically pick up new files.
- Spark container installs OpenJDK + PySpark at runtime; DuckDB uses Python package.
