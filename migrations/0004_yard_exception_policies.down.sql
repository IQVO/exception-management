-- 0004_yard_exception_policies.down.sql: recreate the previous schema exactly.
DELETE FROM exception_policies
WHERE source_type IN (
    'com.warehouse.wms.yard-management.yardvisit.VisitRejected',
    'com.warehouse.wms.yard-management.trailer.SealExceptionRaised',
    'com.warehouse.wms.yard-management.trailer.TrailerDwellExceeded',
    'com.warehouse.wms.yard-management.yardcheck.YardCheckClosed',
    'com.warehouse.wms.dock-scheduling.dockappointment.DockAppointmentNoShow'
);
