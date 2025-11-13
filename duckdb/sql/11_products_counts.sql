SELECT Category, COUNT(*) AS ProductCount
FROM read_csv('/workspace/data/product_data/products.csv', header=true)
GROUP BY Category
ORDER BY ProductCount DESC;
