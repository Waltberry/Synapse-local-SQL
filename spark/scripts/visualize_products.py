# spark/scripts/visualize_products.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, expr, round as sround
import matplotlib
matplotlib.use("Agg")  # headless backend for Docker
import matplotlib.pyplot as plt
import os

OUT_DIR = "/workspace/outputs"
PRODUCTS_CSV = "/workspace/data/product_data/products.csv"
ORDERS_PARQUET = "/workspace/data/parquet/orders"  # created by duckdb run_all

spark = SparkSession.builder.appName("visualize-products").getOrCreate()

# --- 1) Products: count by Category (mirrors the lab chart) ---
df = spark.read.csv(PRODUCTS_CSV, header=True, inferSchema=True)
counts = (
    df.select("Category", "ProductID")
      .groupBy("Category")
      .agg(count("ProductID").alias("ProductCount"))
      .orderBy("Category")
)

# Save table as CSV (for inspection)
counts.coalesce(1).write.mode("overwrite").option("header", True)\
      .csv(os.path.join(OUT_DIR, "spark_products_counts.csv_dir"))

# Plot
pdf = counts.toPandas()
plt.figure(figsize=(12, 6))
plt.bar(pdf["Category"], pdf["ProductCount"])
plt.xticks(rotation=60, ha="right")
plt.title("Product Counts by Category")
plt.xlabel("Category")
plt.ylabel("Products")
plt.tight_layout()
out_img = os.path.join(OUT_DIR, "products_by_category.png")
plt.savefig(out_img, dpi=150)
print(f"Saved chart → {out_img}")

# --- 2) Bonus: revenue by year from Parquet partitions (serverless-style) ---
if os.path.exists(ORDERS_PARQUET):
    orders = spark.read.parquet(ORDERS_PARQUET)
    revenue = (
        orders.groupBy("year")
              .agg(sround(expr("sum(Quantity * UnitPrice + TaxAmount)"), 2)
              .alias("GrossRevenue"))
              .orderBy("year")
    )
    # Save table
    revenue.coalesce(1).write.mode("overwrite").option("header", True)\
           .csv(os.path.join(OUT_DIR, "spark_yearly_revenue.csv_dir"))
    # Plot
    rpdf = revenue.toPandas()
    plt.figure(figsize=(8, 5))
    plt.bar(rpdf["year"], rpdf["GrossRevenue"])
    plt.title("Gross Revenue by Year (Parquet partitions)")
    plt.xlabel("Year")
    plt.ylabel("Gross Revenue")
    plt.tight_layout()
    out_img2 = os.path.join(OUT_DIR, "revenue_by_year.png")
    plt.savefig(out_img2, dpi=150)
    print(f"Saved chart → {out_img2}")
else:
    print(f"Skip revenue plot: {ORDERS_PARQUET} not found. Run DuckDB first.")

spark.stop()
