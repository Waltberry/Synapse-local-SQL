-- Temp views that point at CSV files (header + schema inference)

-- Customer
CREATE OR REPLACE TEMP VIEW Customer_v
USING csv
OPTIONS (path '/workspace/data/lake/RetailDB/Customer/customer.csv', header 'true', inferSchema 'true');

-- Product
CREATE OR REPLACE TEMP VIEW Product_v
USING csv
OPTIONS (path '/workspace/data/lake/RetailDB/Product/product.csv', header 'true', inferSchema 'true');

-- SalesOrder
CREATE OR REPLACE TEMP VIEW SalesOrder_v
USING csv
OPTIONS (path '/workspace/data/lake/RetailDB/SalesOrder/salesorder.csv', header 'true', inferSchema 'true');
