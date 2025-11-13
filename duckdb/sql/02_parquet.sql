-- 02_parquet.sql
-- Parquet came from CSV and was partitioned by 'year' automatically.
SELECT year, COUNT(*) AS RowsInYear, SUM(Quantity) AS Items
FROM orders_parquet
GROUP BY year
ORDER BY year;

-- Partition pruning example: pick 2019 and 2020 only
SELECT year, ROUND(SUM(Quantity * UnitPrice + TaxAmount),2) AS GrossRevenue
FROM orders_parquet
WHERE year IN ('2019','2020')
GROUP BY year
ORDER BY year;
