-- 03_json.sql
-- JSON read auto-infers schema from NDJSON (orders.jsonl)
SELECT SalesOrderNumber, CustomerName, Item, Quantity,
       ROUND(Quantity * UnitPrice + TaxAmount, 2) AS Gross
FROM orders_json
ORDER BY Gross DESC;
