-- Read directly from the file (OPENROWSET analog)
SELECT *
FROM read_csv('/workspace/data/product_data/products.csv', header=true)
LIMIT 100;
