-- 03_json.sql
-- JSON Lines view: orders_json
SELECT SalesOrderNumber, CustomerName, Item, Quantity,
       ROUND(Quantity * UnitPrice + TaxAmount, 2) AS Gross
FROM orders_json
ORDER BY Gross DESC;
