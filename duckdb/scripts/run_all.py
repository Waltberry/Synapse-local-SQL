#run_all.py
#!/usr/bin/env python
import duckdb, os, glob, pathlib

BASE = pathlib.Path("/workspace")

DATA = BASE / "data"
SQL_DIR = BASE / "duckdb" / "sql"
OUT = BASE / "outputs"
OUT.mkdir(parents=True, exist_ok=True)


(os.PathLike,)  

(BASE / "data" / "parquet" / "orders").mkdir(parents=True, exist_ok=True)


con = duckdb.connect(database=":memory:")

print("==> Creating view over CSV (auto-infer schema)...")
con.execute("""
CREATE OR REPLACE VIEW sales_csv AS
SELECT * FROM read_csv_auto('{}');
""".format(str(DATA / "csv" / "*.csv").replace("\\","/")))

print("==> Create partitioned Parquet from CSV (adds year column)...")
con.execute("""
CREATE OR REPLACE TABLE sales_aug AS
SELECT *, strftime(CAST(OrderDate AS DATE), '%Y') AS year FROM sales_csv;
""")
con.execute("""
COPY sales_aug TO '{}' (FORMAT PARQUET, PARTITION_BY (year), OVERWRITE_OR_IGNORE 1);
""".format(str(DATA / "parquet" / "orders").replace("\\","/")))

print("==> Create views for JSON & Parquet...")
con.execute("CREATE OR REPLACE VIEW orders_parquet AS SELECT * FROM read_parquet('{}');".format(
    str(DATA / "parquet" / "orders" / "*" / "*.parquet").replace("\\","/")
))
con.execute("CREATE OR REPLACE VIEW orders_json AS SELECT * FROM read_json_auto('{}');".format(
    str(DATA / "json" / "orders.jsonl").replace("\\","/")
))

# Run all SQL files in order
sql_files = sorted(glob.glob(str(SQL_DIR / "*.sql")))
for path in sql_files:
    name = os.path.basename(path)
    print(f"\n==> Running {name}")
    with open(path, "r") as f:
        sql = f.read()
    statements = [s.strip() for s in sql.split(";") if s.strip()]
    for stmt in statements:
        try:
            res = con.execute(stmt)
            if res.description is not None:
                df = res.df()
                print(df.head(20).to_string(index=False))
                safe = name.replace(".sql","")
                OUT.joinpath(f"duckdb_{safe}.csv").write_text(df.to_csv(index=False))
        except Exception as e:
            print(f"[WARN] Statement failed: {e}\n{stmt}\n")
print("\nDuckDB run complete. Results saved in /workspace/outputs.")
