-- Join across the three “tables” (views)
SELECT
  o.SalesOrderId,
  c.EmailAddress,
  p.ProductName,
  o.Quantity
FROM RetailDB.SalesOrder AS o
JOIN RetailDB.Customer  AS c ON o.CustomerId = c.CustomerId
JOIN RetailDB.Product   AS p ON o.ProductId  = p.ProductId
ORDER BY o.SalesOrderId, o.LineItemId
LIMIT 100;
