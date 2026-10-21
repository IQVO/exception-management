# sla.feature — the bounded SLA sweeper (ADR 0002): a breach sets the
# ESCALATED flag and raises ExceptionEscalated exactly once per case; the
# flag never blocks work (the case stays in the open caseload and keeps
# accepting transitions), and a resolved case is never swept.
Feature: SLA breach escalates without blocking work

  Background:
    Given the clock is 2026-10-21T09:00:00Z
    And a policy is declared for source type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed with severity HIGH queue ps-main and sla 30 minutes
    And a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-1 and payload {"task_id":"task-8a1f"} arrives

  Scenario: The sweeper before the deadline does nothing
    When time advances 29 minutes
    And the SLA sweeper runs
    Then the case is not escalated
    And no escalation event is published

  Scenario: A breach flags ESCALATED and raises the event exactly once
    When time advances 30 minutes
    And the SLA sweeper runs
    Then the case is escalated
    And the published events are ExceptionOpened, ExceptionEscalated

    When the SLA sweeper runs
    Then the case is escalated
    And the published events are ExceptionOpened, ExceptionEscalated

  Scenario: An escalated case keeps working
    When time advances 30 minutes
    And the SLA sweeper runs
    And operator op-9 assigns the case to op-7
    Then the case status is ASSIGNED
    And the case is escalated
    And the open caseload has 1 case
    And the published events are ExceptionOpened, ExceptionEscalated, ExceptionAssigned

  Scenario: A resolved case stays resolved and is never swept
    When operator op-7 resolves the case with code DONE and root cause handled
    And time advances 2 hours
    And the SLA sweeper runs
    Then the case is not escalated
    And the open caseload has 0 cases
    And the published events are ExceptionOpened, ExceptionResolved
