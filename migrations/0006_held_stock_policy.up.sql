-- 0006_held_stock_policy.up.sql: the default routing of inventory-storage's
-- held-stock facts (ADR 0012). A held unit is disposition work nobody owned
-- before: a StockHeld fact opens one case per held unit id, so the
-- follow-up is visible and escalatable; the case auto-resolves when the
-- hold ends (a closing disposition or a release) — that close is
-- definitional (ADR 0012 point 4), so no auto-resolve RULE is declared
-- here. MEDIUM: real money parked on the floor, not a ship-stopping
-- emergency. Queue problem-solve: the disposition decision is a
-- problem-solver's call, in step with ADR 0011. SLA 1440 minutes: a held
-- unit should be dispositioned within a day of booking. Version 1 only
-- (append-only history, ADR 0002): operators redeclare through the policy
-- API, which appends version 2+. ON CONFLICT keeps a re-run safe.
INSERT INTO exception_policies (source_type, version, severity, queue, sla_minutes, declared_at)
VALUES
    ('com.warehouse.wms.inventory-storage.stock.StockHeld', 1, 'MEDIUM', 'problem-solve', 1440, now())
ON CONFLICT (source_type, version) DO NOTHING;
