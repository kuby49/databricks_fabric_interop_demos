-- DESIGN TEMPLATE ONLY: unresolved placeholders make this non-executable.
-- Select the actual mirrored objects and verify mapping, readiness and access
-- separately for each engine. This comparison does not prove that mapping.
WITH expected AS (
  SELECT d.DateKey, d.Date, d.CalendarYear, d.CalendarMonth, d.MonthName,
         d.DayOfMonth, d.IsWeekend, p.ProductKey, p.ProductName, p.Category,
         c.Region, c.Segment, SUM(f.SalesAmount) AS TotalSales,
         SUM(f.Quantity) AS TotalQuantity, COUNT(*) AS SalesLineCount
  FROM {{source_factsales}} f
  JOIN {{source_dimdate}} d ON f.DateKey = d.DateKey
  JOIN {{source_dimproduct}} p ON f.ProductKey = p.ProductKey
  JOIN {{source_dimcustomer}} c ON f.CustomerKey = c.CustomerKey
  GROUP BY d.DateKey, d.Date, d.CalendarYear, d.CalendarMonth, d.MonthName,
           d.DayOfMonth, d.IsWeekend, p.ProductKey, p.ProductName, p.Category,
           c.Region, c.Segment
),
actual AS (
  SELECT DateKey, Date, CalendarYear, CalendarMonth, MonthName, DayOfMonth,
         IsWeekend, ProductKey, ProductName, Category, Region, Segment,
         TotalSales, TotalQuantity, SalesLineCount
  FROM {{mirrored_sales_mv}}
)
-- Compare in both directions so extra and missing groups also count.
SELECT 'missing_or_different' AS Difference, COUNT(*) AS MismatchCount
FROM (SELECT * FROM expected EXCEPT SELECT * FROM actual) differences
UNION ALL
SELECT 'extra_or_different', COUNT(*)
FROM (SELECT * FROM actual EXCEPT SELECT * FROM expected) differences;

-- Check duplicates separately: EXCEPT is set-based and cannot detect them.
SELECT DateKey, ProductKey, Region, Segment, COUNT(*) AS DuplicateCount
FROM {{mirrored_sales_mv}}
GROUP BY DateKey, ProductKey, Region, Segment
HAVING COUNT(*) <> 1;
