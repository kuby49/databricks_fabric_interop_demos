-- Replace placeholders; use discovered schema/table spelling.
CREATE CONNECTION demo_onelake_connection TYPE onelake
OPTIONS (workspace '<dedicated-workspace-guid>', credential '<verified-read-only-credential>');

CREATE FOREIGN CATALOG demo_fabric_sales USING CONNECTION demo_onelake_connection
OPTIONS (data_item '<lakehouse-guid>', item_type 'Lakehouse',
         create_volume_for_lakehouse_files 'false');

SHOW SCHEMAS IN demo_fabric_sales;
SHOW TABLES IN demo_fabric_sales.<discovered-schema>;
SELECT COUNT(*), SUM(SalesAmount) FROM demo_fabric_sales.<discovered-schema>.<discovered-fact-table>;
-- Count/total is a presentation query, not full row-equality verification.
-- No writes to foreign tables; append only in the guarded Fabric producer.
