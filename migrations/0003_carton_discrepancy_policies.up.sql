-- 0003_carton_discrepancy_policies.up.sql: the routing table of
-- inbound-receiving's CartonDiscrepancyRaised (contract R12). Each route is
-- a policy source type; the consumer maps the payload `kind` to a route.
-- Version 1 only (append-only history, ADR 0002): operators redeclare through
-- the policy API, which appends version 2+. ON CONFLICT keeps a re-run safe.
INSERT INTO exception_policies (source_type, version, severity, queue, sla_minutes, declared_at)
VALUES
    ('com.warehouse.wms.inbound-receiving.receipt.CartonDiscrepancyRaised#problem-solve',  1, 'HIGH', 'problem-solve',  60,  now()),
    ('com.warehouse.wms.inbound-receiving.receipt.CartonDiscrepancyRaised#receiving-lead', 1, 'LOW',  'receiving-lead', 480, now())
ON CONFLICT (source_type, version) DO NOTHING;
