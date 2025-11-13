#bootstrap_and_run.py
#!/usr/bin/env python
import os, glob, pathlib
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, year, col, round as sround
# import shutil, os
# for p in [DATA/'cetas'/'productsales', DATA/'cetas'/'yearlysales']:
#     shutil.rmtree(p, ignore_errors=True)


BASE = pathlib.Path("/workspace")

DATA = BASE / "data"
SQL_DIR = BASE / "spark" / "sql"
OUT = BASE / "outputs"
OUT.mkdir(parents=True, exist_ok=True)

spark = (SparkSession.builder
         .appName("local-synapse-style")
         .master("local[*]")
         .config("spark.sql.session.timeZone", "UTC")
         .getOrCreate())

# 1) Read CSV and create temp view
csv_path = str(DATA / "csv" / "*.csv")
df_csv = (spark.read
          .option("header", True)
          .option("inferSchema", True)
          .csv(csv_path))
df_csv.createOrReplaceTempView("sales_csv")

# 2) Write Parquet partitioned by year, then read back
df_aug = (df_csv
          .withColumn("OrderDate", to_date(col("OrderDate")))
          .withColumn("year", year(col("OrderDate")).cast("string")))
parquet_base = str(DATA / "parquet" / "orders")
df_aug.write.mode("overwrite").partitionBy("year").parquet(parquet_base)
df_parquet = spark.read.parquet(parquet_base)
df_parquet.createOrReplaceTempView("orders_parquet")

# 3) Read JSON Lines and create view
json_path = str(DATA / "json" / "orders.jsonl")
df_json = spark.read.json(json_path)
df_json.createOrReplaceTempView("orders_json")

# 4) Run SQL scripts
def run_sql_file(path):
    with open(path, "r") as f:
        sql_text = f.read()
    statements = [s.strip() for s in sql_text.split(";") if s.strip()]
    for stmt in statements:
        print(f"\n==> SPARK SQL: {stmt[:80]}...")
        try:
            df = spark.sql(stmt)
            if df is not None:
                outpath = OUT / f"spark_{os.path.basename(path).replace('.sql','')}.csv"
                df.show(20, truncate=False)
                df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(outpath)+"_dir")
        except Exception as e:
            print(f"[WARN] Statement failed: {e}\n{stmt}\n")

sql_files = sorted(glob.glob(str(SQL_DIR / "*.sql")))
for path in sql_files:
    run_sql_file(path)

print("\nSpark run complete. Results saved in /workspace/outputs.")
spark.stop()
