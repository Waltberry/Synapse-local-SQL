-- Create a schema to hold our views
CREATE SCHEMA IF NOT EXISTS RetailDB;

-- CUSTOMER (no header in CSV, so declare names & types)
CREATE OR REPLACE VIEW RetailDB.Customer AS
SELECT
  CAST(CustomerId  AS BIGINT)  AS CustomerId,
  FirstName::VARCHAR           AS FirstName,
  LastName::VARCHAR            AS LastName,
  EmailAddress::VARCHAR        AS EmailAddress,
  Phone::VARCHAR               AS Phone
FROM read_csv(
  '/workspace/data/lake/RetailDB/Customer/customer.csv',
  columns={
    'CustomerId': 'BIGINT',
    'FirstName':  'VARCHAR',
    'LastName':   'VARCHAR',
    'EmailAddress':'VARCHAR',
    'Phone':      'VARCHAR'
  },
  header=false
);

-- PRODUCT
-- If your product.csv does NOT contain ListPrice, remove the ListPrice line from columns{} and the SELECT.
CREATE OR REPLACE VIEW RetailDB.Product AS
SELECT
  CAST(ProductId AS BIGINT)                 AS ProductId,
  ProductName::VARCHAR                      AS ProductName,
  TRY_CAST(IntroductionDate AS DATE)        AS IntroductionDate,
  TRY_CAST(ActualAbandonmentDate AS DATE)   AS ActualAbandonmentDate,
  TRY_CAST(ProductGrossWeight AS DECIMAL(18,8)) AS ProductGrossWeight,
  ItemSku::VARCHAR                          AS ItemSku,
  TRY_CAST(ListPrice AS DECIMAL(18,2))      AS ListPrice
FROM read_csv(
  '/workspace/data/lake/RetailDB/Product/product.csv',
  columns={
    'ProductId':'BIGINT',
    'ProductName':'VARCHAR',
    'IntroductionDate':'VARCHAR',
    'ActualAbandonmentDate':'VARCHAR',
    'ProductGrossWeight':'VARCHAR',
    'ItemSku':'VARCHAR',
    'ListPrice':'VARCHAR'
  },
  header=false
);

-- SALES ORDER
CREATE OR REPLACE VIEW RetailDB.SalesOrder AS
SELECT
  CAST(SalesOrderId AS BIGINT)       AS SalesOrderId,
  TRY_CAST(OrderDate AS TIMESTAMP)   AS OrderDate,
  CAST(LineItemId AS BIGINT)         AS LineItemId,
  CAST(CustomerId AS BIGINT)         AS CustomerId,
  CAST(ProductId AS BIGINT)          AS ProductId,
  CAST(Quantity AS BIGINT)           AS Quantity
FROM read_csv(
  '/workspace/data/lake/RetailDB/SalesOrder/salesorder.csv',
  columns={
    'SalesOrderId':'BIGINT',
    'OrderDate':'VARCHAR',
    'LineItemId':'BIGINT',
    'CustomerId':'BIGINT',
    'ProductId':'BIGINT',
    'Quantity':'BIGINT'
  },
  header=false
);
