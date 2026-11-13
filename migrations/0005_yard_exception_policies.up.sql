-- 0005_yard_exception_policies.up.sql: the default routing of the yard
-- exceptions (ADR 0010): one policy per source type, so the queue, severity
-- and SLA of a case come from data, not code. yard-management's four
-- exception facts go to the yard supervisor; dock-scheduling's no-show to
-- the dock planner. Version 1 only (append-only history, ADR 0002):
-- operators redeclare through the policy API, which appends version 2+.
-- ON CONFLICT keeps a re-run safe.
INSERT INTO exception_policies (source_type, version, severity, queue, sla_minutes, declared_at)
VALUES
    ('com.warehouse.wms.yard-management.yardvisit.VisitRejected',          1, 'MEDIUM', 'yard-supervisor', 120, now()),
    ('com.warehouse.wms.yard-management.trailer.SealExceptionRaised',      1, 'HIGH',   'yard-supervisor',  30, now()),
    ('com.warehouse.wms.yard-management.trailer.TrailerDwellExceeded',     1, 'MEDIUM', 'yard-supervisor', 240, now()),
    ('com.warehouse.wms.yard-management.yardcheck.YardCheckClosed',        1, 'MEDIUM', 'yard-supervisor', 480, now()),
    ('com.warehouse.wms.dock-scheduling.dockappointment.DockAppointmentNoShow', 1, 'MEDIUM', 'dock-planner', 240, now())
ON CONFLICT (source_type, version) DO NOTHING;
