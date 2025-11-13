-- 01_csv.sql
-- Explore CSV in Spark SQL (view: sales_csv)
SELECT OrderDate, SUM(Quantity) AS Items
FROM sales_csv
GROUP BY OrderDate
ORDER BY OrderDate;

SELECT Item, ROUND(SUM(Quantity * UnitPrice + TaxAmount), 2) AS GrossRevenue
FROM sales_csv
GROUP BY Item
ORDER BY GrossRevenue DESC
LIMIT 10;
