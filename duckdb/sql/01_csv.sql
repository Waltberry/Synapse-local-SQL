-- 01_csv.sql
-- Explore CSV and basic aggregations
SELECT OrderDate, SUM(Quantity) AS Items
FROM sales_csv
GROUP BY OrderDate
ORDER BY OrderDate;

-- Revenue per item (top 10)
SELECT Item, ROUND(SUM(Quantity * UnitPrice + TaxAmount), 2) AS GrossRevenue
FROM sales_csv
GROUP BY Item
ORDER BY GrossRevenue DESC
LIMIT 10;
