-- Product totals -> Parquet directory
INSERT OVERWRITE DIRECTORY '/workspace/data/cetas/productsales'
USING PARQUET
SELECT
  Item AS Product,
  SUM(Quantity) AS ItemsSold,
  ROUND(SUM(UnitPrice) - SUM(TaxAmount), 2) AS NetRevenue
FROM sales_csv
GROUP BY Item;

-- Yearly totals (partitioned). Prefer CTAS with LOCATION for clean partitions:
DROP TABLE IF EXISTS ext_yearlysales;
CREATE TABLE ext_yearlysales
USING PARQUET
PARTITIONED BY (CalendarYear)
LOCATION '/workspace/data/cetas/yearlysales'
AS
SELECT
  YEAR(OrderDate) AS CalendarYear,
  SUM(Quantity) AS ItemsSold,
  ROUND(SUM(UnitPrice) - SUM(TaxAmount), 2) AS NetRevenue
FROM sales_csv
GROUP BY YEAR(OrderDate);

-- Read back for verification (optional views)
CREATE OR REPLACE TEMP VIEW productsales_totals AS
SELECT * FROM parquet.`/workspace/data/cetas/productsales`;

CREATE OR REPLACE TEMP VIEW yearlysales_totals AS
SELECT * FROM parquet.`/workspace/data/cetas/yearlysales`;
