-- Create a mini warehouse schema
CREATE SCHEMA IF NOT EXISTS wh;

-- Dimension: Product
CREATE OR REPLACE TABLE wh.DimProduct AS
SELECT DISTINCT
  Item AS EnglishProductName
FROM read_csv_auto('/workspace/data/csv/*.csv', header=true);

-- Dimension: Date
CREATE OR REPLACE TABLE wh.DimDate AS
SELECT DISTINCT
  CAST(OrderDate AS DATE)                         AS [Date],
  CAST(strftime(CAST(OrderDate AS DATE),'%Y') AS INTEGER) AS CalendarYear,
  CAST(strftime(CAST(OrderDate AS DATE),'%m') AS INTEGER) AS MonthNumberOfYear,
  strftime(CAST(OrderDate AS DATE),'%B')       AS EnglishMonthName
FROM read_csv_auto('/workspace/data/csv/*.csv', header=true);

-- Fact: Internet Sales (minimal mapping)
CREATE OR REPLACE TABLE wh.FactInternetSales AS
SELECT
  CAST(OrderDate AS DATE)   AS OrderDate,
  Item                      AS Product,
  CAST(Quantity AS INTEGER) AS OrderQuantity
FROM read_csv_auto('/workspace/data/csv/*.csv', header=true);

-- Synapse-style join & aggregate by year/month/product
SELECT d.CalendarYear,
       d.MonthNumberOfYear,
       d.EnglishMonthName,
       p.EnglishProductName AS Product,
       SUM(f.OrderQuantity) AS UnitsSold
FROM wh.FactInternetSales f
JOIN wh.DimDate d
  ON f.OrderDate = d.[Date]
JOIN wh.DimProduct p
  ON f.Product = p.EnglishProductName
GROUP BY d.CalendarYear, d.MonthNumberOfYear, d.EnglishMonthName, p.EnglishProductName
ORDER BY d.MonthNumberOfYear;
