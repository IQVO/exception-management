-- 0004_package_missed_load_policy.down.sql: remove the PackageMissedLoad
-- policy seed. A case already opened under it keeps its frozen severity,
-- queue and SLA (the case snapshot never changes); only new facts after the
-- down would find no policy and be WARN-skipped until a redeclaration.
DELETE FROM exception_policies
WHERE source_type = 'com.warehouse.wms.outbound-shipping.shippingpackage.PackageMissedLoad'
  AND version = 1;
