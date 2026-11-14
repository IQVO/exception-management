# sla_rules.feature — documentation-derived rules of the SLA clock and the
# sweeper (use-cases #3 SweepSLABreaches and #11 GetSLAReport, business-context
# "Escalate (sweeper)", ubiquitous-language "SLA clock"/"Escalation"): the
# clock is openedAt + policy.slaMinutes, a breach sets a FLAG once and raises
# ExceptionEscalated, the sweeper reports facts and never blocks work.
Feature: SLA clock and sweeper rules

  Background:
    Given the clock is 2026-10-21T09:00:00Z
    And a policy is declared for source type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed with severity HIGH queue ps-main and sla 30 minutes
    And a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-1 and payload {"task_id":"task-8a1f"} arrives

  Scenario: One second before the deadline the case is not breached; at the deadline it is
    Given the clock is 2026-10-21T09:29:59Z
    When the SLA sweeper runs
    Then the case is not escalated
    And no escalation event is published
    Given the clock is 2026-10-21T09:30:00Z
    When the SLA sweeper runs
    Then the case is escalated

  Scenario: One sweep flags every breached case and only those, each exactly once
    Given the clock is 2026-10-21T09:20:00Z
    And a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-2 at site SIM1 with event id evt-2 and payload {"task_id":"task-9b2c"} arrives
    When the clock is 2026-10-21T09:35:00Z
    And the SLA sweeper runs
    And I look at case case-0001
    Then the case is escalated
    When I look at case case-0002
    Then the case is not escalated
    And the published events are ExceptionOpened, ExceptionOpened, ExceptionEscalated
    When the clock is 2026-10-21T09:50:00Z
    And the SLA sweeper runs
    Then the case is escalated
    And the published events are ExceptionOpened, ExceptionOpened, ExceptionEscalated, ExceptionEscalated
    And the last ExceptionEscalated event has Case case-0002

  Scenario: The escalated flag is not a status: a triaged case stays TRIAGED
    Given operator op-9 triages the case to severity CRITICAL queue ps-urgent
    When time advances 30 minutes
    And the SLA sweeper runs
    Then the case is escalated
    And the case status is TRIAGED
    And the audit trail is OPENED, TRIAGED, ESCALATED
    And the audit entry ESCALATED records actor system
    And the last ExceptionEscalated event has SLADueAt 2026-10-21T09:30:00Z

  Scenario: A breached case can still be resolved
    When time advances 45 minutes
    And the SLA sweeper runs
    And operator op-7 resolves the case with code DONE and root cause handled
    Then the case status is RESOLVED
    And the audit trail is OPENED, ESCALATED, RESOLVED
    And the published events are ExceptionOpened, ExceptionEscalated, ExceptionResolved

  # --- GetSLAReport -------------------------------------------------------------

  Scenario: The SLA report counts a past-due case as breached before the sweeper flags it
    When time advances 40 minutes
    Then the SLA report counts 1 open, 1 breached and 0 escalated cases
    When the SLA sweeper runs
    Then the SLA report counts 1 open, 1 breached and 1 escalated case

  Scenario: The SLA report breaches nothing before the deadline
    When time advances 29 minutes
    Then the SLA report counts 1 open, 0 breached and 0 escalated case

  Scenario: A resolved case leaves the SLA report
    Given a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-2 at site SIM1 with event id evt-2 and payload {"task_id":"task-9b2c"} arrives
    And operator op-7 resolves the case with code DONE and root cause handled
    When time advances 40 minutes
    Then the SLA report counts 1 open, 1 breached and 0 escalated case

  Scenario: The SLA report follows triage in its severity and queue counts
    Given a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-2 at site SIM1 with event id evt-2 and payload {"task_id":"task-9b2c"} arrives
    And I look at case case-0001
    And operator op-9 triages the case to severity CRITICAL queue ps-urgent
    Then the SLA report counts 2 open, 0 breached and 0 escalated cases
    And the SLA report counts 1 open case with severity CRITICAL
    And the SLA report counts 1 open case with severity HIGH
    And the SLA report counts 1 open case in queue ps-urgent
    And the SLA report counts 1 open case in queue ps-main
