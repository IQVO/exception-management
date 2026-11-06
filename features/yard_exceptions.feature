# yard_exceptions.feature - yard-management's VisitRejected, SealExceptionRaised,
# TrailerDwellExceeded and YardCheckClosed, and dock-scheduling's
# DockAppointmentNoShow (ADR 0010): one routed case per source type, per
# discrepancy for a yard check; idempotent by CloudEvents id AND natural key.
Feature: Yard exceptions open routed cases

  Background:
    Given the clock is 2026-11-05T09:00:00Z
    And a policy is declared for source type com.warehouse.wms.yard-management.yardvisit.VisitRejected with severity MEDIUM queue yard-supervisor and sla 120 minutes
    And a policy is declared for source type com.warehouse.wms.yard-management.trailer.SealExceptionRaised with severity HIGH queue yard-supervisor and sla 30 minutes
    And a policy is declared for source type com.warehouse.wms.yard-management.trailer.TrailerDwellExceeded with severity MEDIUM queue yard-supervisor and sla 240 minutes
    And a policy is declared for source type com.warehouse.wms.yard-management.yardcheck.YardCheckClosed with severity MEDIUM queue yard-supervisor and sla 480 minutes
    And a policy is declared for source type com.warehouse.wms.dock-scheduling.dockappointment.DockAppointmentNoShow with severity MEDIUM queue dock-planner and sla 240 minutes

  Scenario: A rejected visit opens a case for the yard supervisor
    When a visit rejection for visit visit-1 at site SITE-1 with event id evt-1 arrives
    Then a new case is opened
    And the case queue is yard-supervisor
    And the case severity is MEDIUM
    And the case sla is 120 minutes
    And the case occurrence is 1
    And the published events are ExceptionOpened

  Scenario: A replayed visit rejection changes nothing
    When a visit rejection for visit visit-1 at site SITE-1 with event id evt-1 arrives
    And a visit rejection for visit visit-1 at site SITE-1 with event id evt-1 arrives
    Then no new case is opened
    And the case occurrence is 1

  Scenario: A seal mismatch is the highest priority yard case
    When a seal exception for trailer trl-1 raised at 2026-11-05T08:30:00Z at site SITE-1 with event id evt-1 arrives
    Then a new case is opened
    And the case queue is yard-supervisor
    And the case severity is HIGH
    And the case sla is 30 minutes

  Scenario: The same seal raise published again touches the case
    When a seal exception for trailer trl-1 raised at 2026-11-05T08:30:00Z at site SITE-1 with event id evt-1 arrives
    And a seal exception for trailer trl-1 raised at 2026-11-05T08:30:00Z at site SITE-1 with event id evt-2 arrives
    Then no new case is opened
    And the case occurrence is 2

  Scenario: The same trailer mismatching on a later visit opens another case
    When a seal exception for trailer trl-1 raised at 2026-11-05T08:30:00Z at site SITE-1 with event id evt-1 arrives
    And a seal exception for trailer trl-1 raised at 2026-11-07T14:00:00Z at site SITE-1 with event id evt-2 arrives
    Then 2 cases are opened
    And the open caseload has 2 cases

  Scenario: A dwell breach opens a case
    When a dwell exceedance for trailer trl-1 at 2026-11-05T08:00:00Z at site SITE-1 with event id evt-1 arrives
    Then a new case is opened
    And the case queue is yard-supervisor
    And the case sla is 240 minutes

  Scenario: Every discrepancy of a yard check opens its own case
    When yard check yck-1 at site SITE-1 closes with event id evt-1 and discrepancies MISSING:SPOT-01:trl-1, UNEXPECTED:SPOT-02:TR200, WRONG_LOCATION:SPOT-03:trl-3
    Then 3 cases are opened
    And the open caseload has 3 cases
    And the case queue is yard-supervisor
    And the case sla is 480 minutes

  Scenario: A replayed yard check closure opens nothing more
    When yard check yck-1 at site SITE-1 closes with event id evt-1 and discrepancies MISSING:SPOT-01:trl-1, UNEXPECTED:SPOT-02:TR200
    And yard check yck-1 at site SITE-1 closes with event id evt-1 and discrepancies MISSING:SPOT-01:trl-1, UNEXPECTED:SPOT-02:TR200
    Then 2 cases are opened
    And the open caseload has 2 cases

  Scenario: A clean yard check opens no case
    When yard check yck-2 at site SITE-1 closes with event id evt-1 and no discrepancies
    Then 0 cases are opened
    And the open caseload has 0 cases

  Scenario: A discrepancy kind the producer adds later is never dropped
    When yard check yck-3 at site SITE-1 closes with event id evt-1 and discrepancies SOMETHING_NEW:SPOT-09:-
    Then a new case is opened
    And the case queue is yard-supervisor

  Scenario: A no-show opens a case for the dock planner
    When a no-show for appointment appt-1 at site SITE-1 with event id evt-1 arrives
    Then a new case is opened
    And the case queue is dock-planner
    And the case severity is MEDIUM
    And the case sla is 240 minutes
    And the published events are ExceptionOpened

  Scenario: A replayed no-show changes nothing
    When a no-show for appointment appt-1 at site SITE-1 with event id evt-1 arrives
    And a no-show for appointment appt-1 at site SITE-1 with event id evt-1 arrives
    Then no new case is opened
    And the case occurrence is 1
