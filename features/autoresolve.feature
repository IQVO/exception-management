# autoresolve.feature — the declarative auto-resolve rule (ADR 0002 and the
# pinned FINAL correlation semantics): ORDER-level match of an
# OrderRepromised fact against the case's work-unit subject, inclusive
# window on the case's last occurrence, fail-soft on no match, and a fact
# arriving after RESOLUTION touches the case but never reopens it.
Feature: Policy auto-resolve on a correlated OrderRepromised

  Background:
    Given the clock is 2026-10-21T09:00:00Z
    And a policy is declared for source type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed with severity HIGH queue ps-main and sla 60 minutes and auto-resolve on com.warehouse.wes.order-management.order.OrderRepromised within 10 minutes

  Scenario: A repromise for the same order within the window auto-resolves the case
    Given a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-1 and payload {"task_id":"task-8a1f"} arrives
    When an OrderRepromised fact for order ord-7 with event id evt-9 arrives
    Then the correlation closes 1 case
    And the case status is RESOLVED
    And the case was auto-resolved by rule com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed->com.warehouse.wes.order-management.order.OrderRepromised with correlated event evt-9
    And the published events are ExceptionOpened, ExceptionAutoResolved

  Scenario: The window edge is inclusive
    Given a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-1 and payload {"task_id":"task-8a1f"} arrives
    When time advances 10 minutes
    And an OrderRepromised fact for order ord-7 with event id evt-9 arrives
    Then the correlation closes 1 case

  Scenario: A repromise after the window does nothing
    Given a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-1 and payload {"task_id":"task-8a1f"} arrives
    When time advances 11 minutes
    And an OrderRepromised fact for order ord-7 with event id evt-9 arrives
    Then the correlation closes 0 cases
    And the case status is OPEN

  Scenario: A repromise for a different order does nothing
    Given a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-1 and payload {"task_id":"task-8a1f"} arrives
    When an OrderRepromised fact for order ord-8 with event id evt-9 arrives
    Then the correlation closes 0 cases
    And the case status is OPEN
    And no auto-resolution event is published

  Scenario: A fact after the auto-resolution touches but never reopens
    Given a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-1 and payload {"task_id":"task-8a1f"} arrives
    When an OrderRepromised fact for order ord-7 with event id evt-9 arrives
    And a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-2 and payload {"task_id":"task-8a1f"} arrives
    Then the case occurrence is 2
    And the case status is RESOLVED
    And no new case is opened
    And the published events are ExceptionOpened, ExceptionAutoResolved
    And the audit trail is OPENED, AUTO_RESOLVED, TOUCHED
