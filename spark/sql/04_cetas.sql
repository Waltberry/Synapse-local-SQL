-- Spark CETAS-style outputs

-- Single “productsales” output (Parquet) – CTAS to a fixed LOCATION
DROP TABLE IF EXISTS ext_productsales;
CREATE TABLE ext_productsales
USING PARQUET
LOCATION '/workspace/data/cetas/productsales'
AS
SELECT
  Item AS Product,
  SUM(Quantity) AS ItemsSold,
  ROUND(SUM(Quantity * UnitPrice + TaxAmount), 2) AS GrossRevenue
FROM sales_csv
GROUP BY Item;

-- Partitioned yearly totals – CTAS with PARTITIONED BY
DROP TABLE IF EXISTS ext_yearlysales;
CREATE TABLE ext_yearlysales
USING PARQUET
PARTITIONED BY (CalendarYear)
LOCATION '/workspace/data/cetas/yearlysales'
AS
SELECT
  YEAR(TO_DATE(OrderDate)) AS CalendarYear,
  SUM(Quantity)            AS ItemsSold,
  ROUND(SUM(Quantity * UnitPrice + TaxAmount), 2) AS NetRevenue
FROM sales_csv
GROUP BY YEAR(TO_DATE(OrderDate));

-- Quick read-back as temp views (verify paths exist)
CREATE OR REPLACE TEMP VIEW productsales_totals AS
SELECT * FROM parquet.`/workspace/data/cetas/productsales`;

CREATE OR REPLACE TEMP VIEW yearlysales_totals AS
SELECT * FROM parquet.`/workspace/data/cetas/yearlysales`;
