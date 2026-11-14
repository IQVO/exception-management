# intake_rules.feature — documentation-derived rules of the open-or-touch decision
# (warehouse-docs exception-management: business-context "The lifecycle" step 1,
# use-cases #1 OpenOrTouchException, aggregate-design-canvas "Natural key",
# "Invariants" and "Lifecycle", domain-events "Consumed", troubleshooting
# "No case opened"). Each scenario names ONE rule.
Feature: Intake rules of the open-or-touch decision

  Background:
    Given the clock is 2026-10-21T09:00:00Z
    And a policy is declared for source type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed with severity HIGH queue ps-main and sla 30 minutes
    And a policy is declared for source type com.warehouse.wes.work-planning.workpool.BacklogThresholdBreached with severity MEDIUM queue planning and sla 120 minutes

  # --- the dedup key (source type, source subject, site) ----------------------

  Scenario Outline: A fact that differs in one part of the dedup key opens its own case
    Given a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-1 and payload {"task_id":"task-8a1f"} arrives
    When a fact of type <type> for work unit <subject> at site <site> with event id evt-2 and payload {"task_id":"task-8a1f"} arrives
    Then 2 cases are opened
    And the open caseload has 2 cases
    And the case occurrence is 1

    Examples:
      | type                                                           | subject      | site |
      | com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed     | ord-7-line-2 | SIM1 |
      | com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed     | ord-7-line-1 | SIM2 |
      | com.warehouse.wes.work-planning.workpool.BacklogThresholdBreached | ord-7-line-1 | SIM1 |

  Scenario: A fact without a site dedupes on the key with an empty site
    Given a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 without a site with event id evt-1 and payload {"task_id":"task-8a1f"} arrives
    And a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 without a site with event id evt-2 and payload {"task_id":"task-8a1f"} arrives
    Then no new case is opened
    And the case occurrence is 2
    When a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-3 and payload {"task_id":"task-8a1f"} arrives
    Then 2 cases are opened
    And the case occurrence is 1
    When a GET request is sent to "/exceptions/case-0001"
    Then the response field "dedupKey.siteId" is null

  Scenario: The use case ignores a replay of the event id that opened the case
    Given a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-1 and payload {"task_id":"task-8a1f"} arrives
    When a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-1 and payload {"task_id":"task-8a1f"} arrives
    Then no new case is opened
    And the case occurrence is 1
    And the audit trail is OPENED
    And the case version is 1

  # --- no policy, no case -----------------------------------------------------

  Scenario: A source type without a declared policy opens nothing until the policy exists
    When a fact of type com.warehouse.wes.network-fulfillment.networkorder.AcknowledgementDeadlineAtRisk for work unit net-ord-9 at site SIM1 with event id evt-1 and payload {"network_ref":"net-ord-9"} is consumed
    Then the command fails with ErrPolicyUndeclared
    And the open caseload has 0 cases
    And 0 cases are opened
    Given a policy is declared for source type com.warehouse.wes.network-fulfillment.networkorder.AcknowledgementDeadlineAtRisk with severity HIGH queue ack-desk and sla 45 minutes
    When a fact of type com.warehouse.wes.network-fulfillment.networkorder.AcknowledgementDeadlineAtRisk for work unit net-ord-9 at site SIM1 with event id evt-2 and payload {"network_ref":"net-ord-9"} is consumed
    Then the consume outcome is applied
    And a new case is opened
    And the case queue is ack-desk
    And the case sla is 45 minutes

  # --- what a touch may and may not change ------------------------------------

  Scenario: A touch never changes the frozen source payload or the opening event
    Given a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-1 and payload {"task_id":"task-8a1f","cpt":"2026-10-21T09:00:00Z"} arrives
    When time advances 10 minutes
    And a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-2 and payload {"task_id":"task-8a1f","cpt":"2026-10-21T09:10:00Z"} arrives
    Then the case occurrence is 2
    And the case was last seen at 2026-10-21T09:10:00Z
    And the frozen payload of the case is {"task_id":"task-8a1f","cpt":"2026-10-21T09:00:00Z"}
    When a GET request is sent to "/exceptions/case-0001"
    Then the response field "sourceEventId" is evt-1
    And the response field "sourcePayload.cpt" is 2026-10-21T09:00:00Z

  Scenario: A touch advances lastSeenAt but never moves the SLA clock
    Given a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-1 and payload {"task_id":"task-8a1f"} arrives
    When time advances 25 minutes
    And a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-2 and payload {"task_id":"task-8a1f"} arrives
    Then the case was last seen at 2026-10-21T09:25:00Z
    And the case sla is due at 2026-10-21T09:30:00Z
    When time advances 5 minutes
    And the SLA sweeper runs
    Then the case is escalated

  Scenario: A touch never rewinds the operator's work
    Given a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-1 and payload {"task_id":"task-8a1f"} arrives
    And operator op-9 triages the case to severity CRITICAL queue ps-urgent
    And operator op-9 assigns the case to ana
    When a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-2 and payload {"task_id":"task-8a1f"} arrives
    Then the case status is ASSIGNED
    And the case severity is CRITICAL
    And the case queue is ps-urgent
    And the case assignee is ana
    And the case occurrence is 2
    And the audit trail is OPENED, TRIAGED, ASSIGNED, TOUCHED
    And the published events are ExceptionOpened, ExceptionTriaged, ExceptionAssigned

  Scenario: A fact after an operator's resolution counts and never reopens the case
    Given a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-1 and payload {"task_id":"task-8a1f"} arrives
    And operator op-7 resolves the case with code RELOCATED and root cause bin-mismatch
    When time advances 3 minutes
    And a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-2 and payload {"task_id":"task-8a1f"} arrives
    Then the case status is RESOLVED
    And the case occurrence is 2
    And the case was last seen at 2026-10-21T09:03:00Z
    And no new case is opened
    And the audit trail is OPENED, RESOLVED, TOUCHED
    And the published events are ExceptionOpened, ExceptionResolved
    When a GET request is sent to "/exceptions"
    Then the response list "items" has 1 items

  # --- policy versions ---------------------------------------------------------

  Scenario: A new policy version governs only the cases opened after it
    Given a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-1 and payload {"task_id":"task-8a1f"} arrives
    When a policy is declared for source type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed with severity LOW queue ps-night and sla 90 minutes
    And time advances 1 minutes
    And a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-8-line-1 at site SIM1 with event id evt-2 and payload {"task_id":"task-9b2c"} arrives
    Then the case severity is LOW
    And the case queue is ps-night
    And the case sla is 90 minutes
    And the case sla is due at 2026-10-21T10:31:00Z
    When I look at case case-0001
    Then the case severity is HIGH
    And the case queue is ps-main
    And the case sla is 30 minutes
    When a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-3 and payload {"task_id":"task-8a1f"} arrives
    Then the case severity is HIGH
    And the case sla is 30 minutes
    And the case occurrence is 2

  # --- the consumed sources of the documentation ------------------------------

  Scenario Outline: Every consumed source opens its case under its own source type's policy
    Given a policy is declared for source type <type> with severity <severity> queue <queue> and sla <sla> minutes
    When a fact of type <type> for work unit <subject> at site SIM1 with event id evt-1 and payload {"ref":"<subject>"} arrives
    Then a new case is opened
    And the case status is OPEN
    And the case severity is <severity>
    And the case queue is <queue>
    And the case sla is <sla> minutes
    And the frozen payload of the case is {"ref":"<subject>"}

    Examples:
      | type      | subject    | severity | queue         | sla |
      | com.warehouse.wes.fulfillment-execution.package.PackageDiverted     | pkg-5001   | HIGH     | problem-solve | 60  |
      | com.warehouse.wes.network-fulfillment.networkorder.AcknowledgementDeadlineAtRisk     | net-ord-9  | CRITICAL | ack-desk      | 15  |
      | com.warehouse.wms.returns-processing.returnreceipt.UnexpectedReturnReceived     | rcpt-330   | LOW      | returns-desk  | 480 |
      | com.warehouse.wms.inventory-storage.bin.DiscrepancyDetected    | bin-A-12-03| MEDIUM   | count-review  | 240 |
