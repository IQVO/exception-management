-- 0003_carton_discrepancy_policies.down.sql: recreate the previous schema exactly.
DELETE FROM exception_policies
WHERE source_type IN (
    'com.warehouse.wms.inbound-receiving.receipt.CartonDiscrepancyRaised#problem-solve',
    'com.warehouse.wms.inbound-receiving.receipt.CartonDiscrepancyRaised#receiving-lead'
);
