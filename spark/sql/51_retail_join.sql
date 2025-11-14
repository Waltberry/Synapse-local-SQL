-- Join across the three “tables” (views)
SELECT
  o.SalesOrderId,
  c.EmailAddress,
  p.ProductName,
  o.Quantity
FROM SalesOrder_v o
JOIN Customer_v  c ON o.CustomerId = c.CustomerId
JOIN Product_v   p ON o.ProductId  = p.ProductId
ORDER BY o.SalesOrderId, o.LineItemId
LIMIT 100;
