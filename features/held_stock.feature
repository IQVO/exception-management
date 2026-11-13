# held_stock.feature — the held-stock exception cases of ADR 0012: a
# StockHeld fact opens exactly ONE case per held unit id (a re-emitted hold
# touches it), a disposition that ends the hold or a release auto-resolves
# it with the outcome in the resolution note, a REWORK disposition keeps
# the unit held and therefore the case open, and a closing fact for a unit
# we never saw held is a fail-soft no-op.
Feature: Held-stock exception cases

  Background:
    Given the clock is 2026-11-12T09:00:00Z
    And a policy is declared for source type com.warehouse.wms.inventory-storage.stock.StockHeld with severity MEDIUM queue problem-solve and sla 1440 minutes

  Scenario: A held unit opens one case that freezes the payload
    When a StockHeld fact for held unit hu-1 with event id hold-1 and payload {"held_unit_id":"hu-1","sku":"SKU-9","quantity":4,"reason":"DAMAGED","receipt_id":"rcpt-77","asn_number":"ASN-88","line_no":2} arrives
    Then a new case is opened
    And the case status is OPEN
    And the case severity is MEDIUM
    And the case queue is problem-solve
    And the case occurrence is 1
    And the frozen payload of the case is {"asn_number":"ASN-88","held_unit_id":"hu-1","line_no":2,"quantity":4,"reason":"DAMAGED","receipt_id":"rcpt-77","sku":"SKU-9"}
    And the published events are ExceptionOpened

  Scenario: A re-emitted StockHeld touches, it never opens a second case
    When a StockHeld fact for held unit hu-2 with event id hold-2 and payload {"held_unit_id":"hu-2","sku":"SKU-9","quantity":1,"reason":"DAMAGED","receipt_id":"r","asn_number":"a","line_no":1} arrives
    And a StockHeld fact for held unit hu-2 with event id hold-3 and payload {"held_unit_id":"hu-2","sku":"SKU-9","quantity":1,"reason":"DAMAGED","receipt_id":"r","asn_number":"a","line_no":1} arrives
    Then no new case is opened
    And the case occurrence is 2
    And the audit trail is OPENED, TOUCHED

  Scenario: A disposition that ends the hold auto-resolves with the outcome in the note
    Given a StockHeld fact for held unit hu-3 with event id hold-4 and payload {"held_unit_id":"hu-3","sku":"SKU-9","quantity":2,"reason":"DAMAGED","receipt_id":"r","asn_number":"a","line_no":1} arrives
    When a StockDispositioned fact with disposition RETURN_TO_VENDOR for held unit hu-3 with event id disp-1 arrives
    Then the case status is RESOLVED
    And the case was auto-resolved by rule com.warehouse.wms.inventory-storage.stock.StockHeld->com.warehouse.wms.inventory-storage.stock.StockDispositioned with correlated event disp-1
    And the case resolution note is disposition RETURN_TO_VENDOR
    And the published events are ExceptionOpened, ExceptionAutoResolved
    And the audit trail is OPENED, AUTO_RESOLVED

  Scenario: A REWORK disposition keeps the case open
    Given a StockHeld fact for held unit hu-4 with event id hold-5 and payload {"held_unit_id":"hu-4","sku":"SKU-9","quantity":2,"reason":"DAMAGED","receipt_id":"r","asn_number":"a","line_no":1} arrives
    When a StockDispositioned fact with disposition REWORK for held unit hu-4 with event id disp-2 arrives
    Then the case status is OPEN
    And the case occurrence is 2
    And no auto-resolution event is published
    And the audit trail is OPENED, TOUCHED

  Scenario: A release auto-resolves; its paired disposition changes nothing
    Given a StockHeld fact for held unit hu-5 with event id hold-6 and payload {"held_unit_id":"hu-5","sku":"SKU-9","quantity":3,"reason":"DAMAGED","receipt_id":"r","asn_number":"a","line_no":1} arrives
    When a StockReleased fact for held unit hu-5 with event id rel-1 arrives
    Then the case status is RESOLVED
    And the case resolution note is stock released
    And a StockDispositioned fact with disposition RELEASE for held unit hu-5 with event id disp-3 arrives
    And the case status is RESOLVED
    And the published events are ExceptionOpened, ExceptionAutoResolved

  Scenario: A closing fact for an unknown held unit is a fail-soft no-op
    When a StockDispositioned fact with disposition SCRAP for held unit hu-ghost with event id disp-9 arrives
    Then the open caseload has 0 cases
    And no auto-resolution event is published
