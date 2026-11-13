# consumers.feature — the fleet dedupe rule made concrete (wave 2):
# ConsumeFact claims the fact's CloudEvents id and applies open-or-touch in
# ONE unit of work, so a redelivered offset is a duplicate that changes
# nothing, a fact without an id is refused before any claim, and a fact that
# fails after the claim is re-processed, not skipped.
Feature: Consume exception facts with exactly-once effects

  Background:
    Given the clock is 2026-10-21T09:00:00Z
    And a policy is declared for source type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed with severity HIGH queue ps-main and sla 30 minutes

  Scenario: A consumed fact opens the case; its redelivery is a duplicate
    When a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-1 and payload {"task_id":"task-8a1f"} is consumed
    Then the consume outcome is applied
    And a new case is opened
    And the case status is OPEN
    When a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-1 and payload {"task_id":"task-8a1f"} is consumed
    Then the consume outcome is duplicate
    And no new case is opened
    And the case occurrence is 1

  Scenario: A distinct fact under the same key touches through the consumer too
    When a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-1 and payload {"task_id":"task-8a1f"} is consumed
    And a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-2 and payload {"task_id":"task-8a1f"} is consumed
    Then the consume outcome is applied
    And no new case is opened
    And the case occurrence is 2

  Scenario: A fact without a CloudEvents id is refused before any claim
    When a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id  and payload {"task_id":"task-8a1f"} is consumed
    Then the command fails with ErrNoEventID
    And the open caseload has 0 cases

  Scenario: A consumed case is operable over the API
    When a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-1 and payload {"task_id":"task-8a1f"} is consumed
    Then the consume outcome is applied
    When a GET request is sent to "/exceptions/case-0001"
    Then the response status is 200
    And the response field "dedupKey.sourceType" is com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed
    And the response field "dedupKey.sourceSubject" is ord-7-line-1
    When a POST request is sent to "/exceptions/case-0001/assignments" with key "asg-1" as operator-1 and the JSON body {"assignee":"ana"}
    Then the response status is 200
    And the response field "status" is ASSIGNED
