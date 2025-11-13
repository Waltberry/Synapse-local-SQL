-- 02_parquet.sql
-- orders_parquet has 'year' column from partition directory
SELECT year, COUNT(*) AS RowsInYear, SUM(Quantity) AS Items
FROM orders_parquet
GROUP BY year
ORDER BY year;

SELECT year, ROUND(SUM(Quantity * UnitPrice + TaxAmount), 2) AS GrossRevenue
FROM orders_parquet
WHERE year IN ('2019','2020')
GROUP BY year
ORDER BY year;
