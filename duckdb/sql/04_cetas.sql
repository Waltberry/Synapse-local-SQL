-- Product totals -> Parquet (CETAS-style)
-- Writes to /workspace/data/cetas/productsales/*.parquet
COPY (
  SELECT
    Item AS Product,
    SUM(Quantity) AS ItemsSold,
    ROUND(SUM(UnitPrice) - SUM(TaxAmount), 2) AS NetRevenue
  FROM sales_csv           -- already created by run_all.py
  GROUP BY 1
)
TO '/workspace/data/cetas/productsales'
(FORMAT PARQUET, OVERWRITE_OR_IGNORE 1);

-- Yearly totals (parameterizable “proc” feel via a macro)
CREATE OR REPLACE MACRO yearly_sales(y STRING) AS TABLE (
  SELECT
    strftime(OrderDate, '%Y') AS CalendarYear,
    SUM(Quantity) AS ItemsSold,
    ROUND(SUM(UnitPrice) - SUM(TaxAmount), 2) AS NetRevenue
  FROM sales_csv
  WHERE strftime(OrderDate, '%Y') = y
  GROUP BY 1
);

-- Write one year to partitioned Parquet (folder per CalendarYear)
COPY (
  SELECT * FROM yearly_sales('2020')
)
TO '/workspace/data/cetas/yearlysales'
(FORMAT PARQUET, PARTITION_BY (CalendarYear), OVERWRITE_OR_IGNORE 1);

-- “External table” feel = read back as a view
CREATE OR REPLACE VIEW productsales_totals AS
SELECT * FROM read_parquet('/workspace/data/cetas/productsales/*.parquet');

CREATE OR REPLACE VIEW yearlysales_totals AS
SELECT * FROM read_parquet('/workspace/data/cetas/yearlysales/*/*.parquet');
