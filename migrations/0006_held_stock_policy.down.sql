-- 0006_held_stock_policy.down.sql: recreate the previous schema exactly —
-- no held-stock policy rows. With no declared version a StockHeld fact is
-- the ADR 0002 policy skip (WARN, commit past).
DELETE FROM exception_policies
WHERE source_type = 'com.warehouse.wms.inventory-storage.stock.StockHeld';
