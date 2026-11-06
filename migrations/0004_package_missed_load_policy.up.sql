-- 0004_package_missed_load_policy.up.sql: the policy of outbound-shipping's
-- PackageMissedLoad (ADR 0010). One route: a package that missed its load is
-- a problem-solver's case — the operator decides the package's next load (or
-- its return to stock) while order-management re-promises the order from the
-- same fact. Version 1 only (append-only history, ADR 0002): operators
-- redeclare through the policy API, which appends version 2+. ON CONFLICT
-- keeps a re-run safe.
INSERT INTO exception_policies (source_type, version, severity, queue, sla_minutes, declared_at)
VALUES
    ('com.warehouse.wms.outbound-shipping.shippingpackage.PackageMissedLoad', 1, 'HIGH', 'problem-solve', 60, now())
ON CONFLICT (source_type, version) DO NOTHING;
