-- Simple “warehouse-style” dims/fact over the lake files

CREATE SCHEMA IF NOT EXISTS wh;

-- Dimension: Date
CREATE OR REPLACE TABLE wh.DimDate AS
SELECT DISTINCT
  CAST(OrderDate AS DATE)                                        AS "Date",
  CAST(strftime(CAST(OrderDate AS DATE),'%Y') AS INTEGER)        AS CalendarYear,
  CAST(strftime(CAST(OrderDate AS DATE),'%m') AS INTEGER)        AS MonthNumberOfYear,
  strftime(CAST(OrderDate AS DATE),'%B')                         AS EnglishMonthName
FROM read_csv_auto('/workspace/data/csv/*.csv', header=true);

-- Dimension: Product
CREATE OR REPLACE TABLE wh.DimProduct AS
SELECT DISTINCT
  Item AS ProductName
FROM read_csv_auto('/workspace/data/csv/*.csv', header=true);

-- Fact: InternetSales-like (using our sales CSVs)
CREATE OR REPLACE TABLE wh.FactInternetSales AS
SELECT
  CAST(OrderDate AS DATE)                 AS OrderDate,
  Item                                    AS Product,
  CAST(Quantity AS INTEGER)               AS OrderQuantity
FROM read_csv_auto('/workspace/data/csv/*.csv', header=true);

-- Example aggregate by year/month/product
SELECT
  d.CalendarYear,
  d.MonthNumberOfYear,
  d.EnglishMonthName,
  p.ProductName,
  SUM(f.OrderQuantity) AS UnitsSold
FROM wh.FactInternetSales f
JOIN wh.DimDate d
  ON f.OrderDate = d."Date"
JOIN wh.DimProduct p
  ON f.Product = p.ProductName
GROUP BY d.CalendarYear, d.MonthNumberOfYear, d.EnglishMonthName, p.ProductName
ORDER BY d.CalendarYear, d.MonthNumberOfYear, p.ProductName
LIMIT 200;
