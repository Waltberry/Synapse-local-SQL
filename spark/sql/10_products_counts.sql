-- Create a temp view from CSV path (DataFrame .read.csv() via SQL)
CREATE OR REPLACE TEMP VIEW products
USING csv
OPTIONS (
  path '/workspace/data/product_data/products.csv',
  header 'true',
  inferSchema 'true'
);

-- Count by Category (like the Studio chart)
SELECT Category, COUNT(*) AS ProductCount
FROM products
GROUP BY Category
ORDER BY ProductCount DESC;
