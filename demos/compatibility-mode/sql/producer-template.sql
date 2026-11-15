-- DESIGN TEMPLATE ONLY: unresolved {{...}} placeholders deliberately make this
-- non-executable. Do not execute while the feasibility gate is blocked.
-- Reconcile exact ownership and current product support before any future use.
-- Do not run over the existing pilot table or use CREATE OR REPLACE.

CREATE SCHEMA {{catalog}}.{{schema}}
MANAGED LOCATION '{{managed_folder}}';

-- Apply this block once to the normal producer schema, never over live data.
-- No LOCATION clause: these tables inherit their schema's managed location.
CREATE TABLE {{catalog}}.{{schema}}.DimDate (
  DateKey INT NOT NULL,
  `Date` DATE,
  CalendarYear INT,
  CalendarMonth INT,
  MonthName STRING,
  DayOfMonth INT,
  IsWeekend BOOLEAN
) USING DELTA TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported');

CREATE TABLE {{catalog}}.{{schema}}.DimProduct (
  ProductKey INT NOT NULL,
  ProductName STRING,
  Category STRING,
  UnitPrice DECIMAL(12,2)
) USING DELTA TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported');

CREATE TABLE {{catalog}}.{{schema}}.DimCustomer (
  CustomerKey INT NOT NULL,
  CustomerName STRING,
  Region STRING,
  Segment STRING
) USING DELTA TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported');

CREATE TABLE {{catalog}}.{{schema}}.FactSales (
  SalesLineKey BIGINT NOT NULL,
  DateKey INT NOT NULL,
  ProductKey INT NOT NULL,
  CustomerKey INT NOT NULL,
  Quantity INT,
  UnitPrice DECIMAL(12,2),
  SalesAmount DECIMAL(14,2)
) USING DELTA TBLPROPERTIES ('delta.feature.catalogManaged' = 'supported');

-- Load the prepared baseline once into the normal schema and validate nulls,
-- duplicates and orphan keys before applying this informational constraint
-- block once. Databricks does not enforce PK/FK keys.
ALTER TABLE {{catalog}}.{{schema}}.DimDate
  ADD CONSTRAINT dimdate_pk PRIMARY KEY (DateKey);
ALTER TABLE {{catalog}}.{{schema}}.DimProduct
  ADD CONSTRAINT dimproduct_pk PRIMARY KEY (ProductKey);
ALTER TABLE {{catalog}}.{{schema}}.DimCustomer
  ADD CONSTRAINT dimcustomer_pk PRIMARY KEY (CustomerKey);
ALTER TABLE {{catalog}}.{{schema}}.FactSales
  ADD CONSTRAINT factsales_pk PRIMARY KEY (SalesLineKey);
ALTER TABLE {{catalog}}.{{schema}}.FactSales
  ADD CONSTRAINT factsales_datekey_fk FOREIGN KEY (DateKey)
  REFERENCES {{catalog}}.{{schema}}.DimDate (DateKey);
ALTER TABLE {{catalog}}.{{schema}}.FactSales
  ADD CONSTRAINT factsales_productkey_fk FOREIGN KEY (ProductKey)
  REFERENCES {{catalog}}.{{schema}}.DimProduct (ProductKey);
ALTER TABLE {{catalog}}.{{schema}}.FactSales
  ADD CONSTRAINT factsales_customerkey_fk FOREIGN KEY (CustomerKey)
  REFERENCES {{catalog}}.{{schema}}.DimCustomer (CustomerKey);

-- Only after each destination's existence, emptiness, seven-day reuse and
-- privileges have been independently verified, enable the compatibility tables.
ALTER TABLE {{catalog}}.{{schema}}.DimDate SET TBLPROPERTIES (
  'delta.universalFormat.enabledFormats' = 'compatibility',
  'delta.universalFormat.compatibility.location' = '{{unique_empty_dimdate_destination}}'
);
ALTER TABLE {{catalog}}.{{schema}}.DimProduct SET TBLPROPERTIES (
  'delta.universalFormat.enabledFormats' = 'compatibility',
  'delta.universalFormat.compatibility.location' = '{{unique_empty_dimproduct_destination}}'
);
ALTER TABLE {{catalog}}.{{schema}}.DimCustomer SET TBLPROPERTIES (
  'delta.universalFormat.enabledFormats' = 'compatibility',
  'delta.universalFormat.compatibility.location' = '{{unique_empty_dimcustomer_destination}}'
);
ALTER TABLE {{catalog}}.{{schema}}.FactSales SET TBLPROPERTIES (
  'delta.universalFormat.enabledFormats' = 'compatibility',
  'delta.universalFormat.compatibility.location' = '{{unique_empty_factsales_destination}}'
);

-- One native MV, using the same normal tables; properties are set at creation.
CREATE MATERIALIZED VIEW {{catalog}}.{{schema}}.sales_by_day_product_region_segment
TBLPROPERTIES (
  'delta.feature.catalogManaged' = 'supported',
  'delta.universalFormat.enabledFormats' = 'compatibility',
  'delta.universalFormat.compatibility.location' = '{{unique_empty_mv_destination}}'
)
AS
SELECT d.DateKey, d.`Date`, d.CalendarYear, d.CalendarMonth, d.MonthName,
       d.DayOfMonth, d.IsWeekend, p.ProductKey, p.ProductName, p.Category,
       c.Region, c.Segment, SUM(f.SalesAmount) AS TotalSales,
       SUM(f.Quantity) AS TotalQuantity, COUNT(*) AS SalesLineCount
FROM {{catalog}}.{{schema}}.FactSales f
JOIN {{catalog}}.{{schema}}.DimDate d ON f.DateKey = d.DateKey
JOIN {{catalog}}.{{schema}}.DimProduct p ON f.ProductKey = p.ProductKey
JOIN {{catalog}}.{{schema}}.DimCustomer c ON f.CustomerKey = c.CustomerKey
GROUP BY d.DateKey, d.`Date`, d.CalendarYear, d.CalendarMonth, d.MonthName,
         d.DayOfMonth, d.IsWeekend, p.ProductKey, p.ProductName, p.Category,
         c.Region, c.Segment;

-- Each compatibility object needs a separate destination.
-- Refresh native MVs and generated representations with supported product APIs;
-- verify success, actual output location and protocol before creating mirrors.
