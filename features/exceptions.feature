# exceptions.feature — the consumer-driven entry point (ADR 0002): a
# consumed source fact opens ONE case per dedup key, freezes the payload and
# inherits severity, queue and SLA from the declared policy version; later
# facts for the same key touch, they never open a second case. This is the
# contract the Kafka consumers (a later step) will drive.
Feature: Open or touch from a consumed fact

  Background:
    Given the clock is 2026-10-21T09:00:00Z
    And a policy is declared for source type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed with severity HIGH queue ps-main and sla 30 minutes

  Scenario: A fact opens a case that freezes the payload and inherits the policy
    When a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-1 and payload {"task_id":"task-8a1f","cpt":"2026-10-21T09:00:00Z"} arrives
    Then a new case is opened
    And the case status is OPEN
    And the case severity is HIGH
    And the case queue is ps-main
    And the case sla is 30 minutes
    And the case sla is due at 2026-10-21T09:30:00Z
    And the case occurrence is 1
    And the frozen payload of the case is {"cpt":"2026-10-21T09:00:00Z","task_id":"task-8a1f"}
    And the published events are ExceptionOpened

  Scenario: A repeated fact for the same key touches, it never re-opens
    When a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-1 and payload {"task_id":"task-8a1f"} arrives
    And a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-2 and payload {"task_id":"task-8a1f"} arrives
    Then no new case is opened
    And the case occurrence is 2
    And the published events are ExceptionOpened

  Scenario: TaskCPTMissed re-fires on every sweep tick and stays one case
    When a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id tick-1 and payload {"task_id":"task-8a1f"} arrives
    And time advances 5 minutes
    And a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id tick-2 and payload {"task_id":"task-8a1f"} arrives
    And time advances 5 minutes
    And a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id tick-3 and payload {"task_id":"task-8a1f"} arrives
    Then no new case is opened
    And the case occurrence is 3
    And the case was last seen at 2026-10-21T09:10:00Z
    And the published events are ExceptionOpened
    And the audit trail is OPENED, TOUCHED, TOUCHED
