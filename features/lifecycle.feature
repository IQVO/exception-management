# lifecycle.feature — the operator lifecycle of a case (ADR 0002): triage,
# assign, resolve; RESOLVED is terminal (the 409-class ErrCaseClosed) and
# every accepted transition records its actor in the audit trail.
Feature: Operator lifecycle of an exception case

  Background:
    Given the clock is 2026-10-21T09:00:00Z
    And a policy is declared for source type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed with severity HIGH queue ps-main and sla 30 minutes
    And a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-1 and payload {"task_id":"task-8a1f"} arrives

  Scenario: Triage, assign, then resolve with a mandatory resolution code
    When operator op-9 triages the case to severity CRITICAL queue ps-urgent
    Then the case status is TRIAGED
    And the case queue is ps-urgent

    When operator op-9 assigns the case to op-7
    Then the case status is ASSIGNED
    And the case assignee is op-7
    And the published events are ExceptionOpened, ExceptionTriaged, ExceptionAssigned

    When operator op-7 resolves the case with code RELOCATED and root cause bin-mismatch
    Then the case status is RESOLVED
    And the case resolution code is RELOCATED
    And the case root cause is bin-mismatch
    And the published events are ExceptionOpened, ExceptionTriaged, ExceptionAssigned, ExceptionResolved
    And the audit trail is OPENED, TRIAGED, ASSIGNED, RESOLVED

  Scenario: A resolution without a code or root cause is refused
    When operator op-7 resolves the case with code "" and root cause bin-mismatch
    Then the command fails with ErrBlankResolutionCode
    And the case status is OPEN

    When operator op-7 resolves the case with code RELOCATED and root cause ""
    Then the command fails with ErrBlankRootCause
    And the case status is OPEN

  Scenario: Resolving twice is a 409-class terminal error
    When operator op-7 resolves the case with code DONE and root cause handled
    And operator op-7 resolves the case with code AGAIN and root cause handled-again
    Then the command fails with ErrCaseClosed
    And the case status is RESOLVED

  Scenario: Every transition records its actor
    When operator op-9 triages the case to severity CRITICAL queue ps-urgent
    And operator op-9 assigns the case to op-7
    And operator op-7 resolves the case with code DONE and root cause handled
    Then the audit entry TRIAGED records actor op-9
    And the audit entry ASSIGNED records actor op-9
    And the audit entry RESOLVED records actor op-7
