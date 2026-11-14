# read_rules.feature — documentation-derived rules of the read side
# (use-cases #8 ListOpenExceptions and #9 GetException: the listing is every
# case whatever its status unless a status filter names one, filters status /
# queue / site / sourceType, one keyset page {items, nextCursor}, limit 1-500,
# newest opened first; the whole case on GET).
Feature: Read rules of the case listing and the case view

  Background:
    Given the clock is 2026-10-21T09:00:00Z
    And a policy is declared for source type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed with severity HIGH queue ps-main and sla 30 minutes
    And a policy is declared for source type com.warehouse.wes.work-planning.workpool.BacklogThresholdBreached with severity MEDIUM queue planning and sla 120 minutes

  Scenario: Without a status filter the listing keeps the resolved cases
    Given a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-1 and payload {"task_id":"task-1"} arrives
    And a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-2 at site SIM1 with event id evt-2 and payload {"task_id":"task-2"} arrives
    And operator op-7 resolves the case with code DONE and root cause handled
    When a GET request is sent to "/exceptions"
    Then the response status is 200
    And the response list "items" has 2 items

  Scenario Outline: The listing narrows by queue, site and source type, and the filters combine
    Given a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-1 and payload {"task_id":"task-1"} arrives
    And a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-2 at site SIM2 with event id evt-2 and payload {"task_id":"task-2"} arrives
    And a fact of type com.warehouse.wes.work-planning.workpool.BacklogThresholdBreached for work unit path-17 at site SIM1 with event id evt-3 and payload {"path_id":"path-17"} arrives
    When a GET request is sent to "/exceptions?<query>"
    Then the response status is 200
    And the response list "items" has <count> items

    Examples:
      | query                              | count |
      | queue=planning                     | 1     |
      | queue=ps-main                      | 2     |
      | site=SIM1                          | 2     |
      | site=SIM2                          | 1     |
      | sourceType=com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed | 2     |
      | queue=planning&site=SIM2           | 0     |
      | queue=nowhere                      | 0     |

  Scenario Outline: The page limit accepts 1 to 500
    When a GET request is sent to "/exceptions?limit=<limit>"
    Then the response status is 200
    And the response list "items" has 0 items

    Examples:
      | limit |
      | 1     |
      | 500   |

  Scenario Outline: A limit outside 1 to 500, or malformed, is refused
    When a GET request is sent to "/exceptions?<query>"
    Then the response status is 400
    And the response problem is "invalid-limit"

    Examples:
      | query           |
      | limit=501       |
      | limit=-1        |
      | limit=abc       |
      | limit=          |
      | limit=1&limit=2 |

  Scenario: The listing is newest opened first and a cursor continues exactly after the last row
    Given a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-1 and payload {"task_id":"task-1"} arrives
    And time advances 5 minutes
    And a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-2 at site SIM1 with event id evt-2 and payload {"task_id":"task-2"} arrives
    And time advances 5 minutes
    And a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-3 at site SIM1 with event id evt-3 and payload {"task_id":"task-3"} arrives
    When a GET request is sent to "/exceptions?limit=2"
    Then the response list "items" has 2 items
    And the response field "items.0.caseId" is case-0003
    And the response field "items.1.caseId" is case-0002
    When a GET request is sent to "/exceptions?limit=2" with the cursor of the previous page
    Then the response list "items" has 1 items
    And the response field "items.0.caseId" is case-0001
    And the response has no field "nextCursor"

  Scenario: A page that exactly fills the limit has no next cursor
    Given a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-1 and payload {"task_id":"task-1"} arrives
    And a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-2 at site SIM1 with event id evt-2 and payload {"task_id":"task-2"} arrives
    When a GET request is sent to "/exceptions?limit=2"
    Then the response list "items" has 2 items
    And the response has no field "nextCursor"

  Scenario: The whole case is readable: key, clock, frozen evidence and audit trail
    Given a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-1 and payload {"task_id":"task-8a1f","order_ref":"ord-7-line-1"} arrives
    When a GET request is sent to "/exceptions/case-0001"
    Then the response status is 200
    And the response field "caseId" is case-0001
    And the response field "dedupKey.sourceType" is com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed
    And the response field "dedupKey.sourceSubject" is ord-7-line-1
    And the response field "dedupKey.siteId" is SIM1
    And the response field "status" is OPEN
    And the response field "severity" is HIGH
    And the response field "queue" is ps-main
    And the response field "occurrenceCount" is 1
    And the response field "escalated" is false
    And the response field "openedAt" is 2026-10-21T09:00:00Z
    And the response field "lastSeenAt" is 2026-10-21T09:00:00Z
    And the response field "slaDueAt" is 2026-10-21T09:30:00Z
    And the response field "sourceEventId" is evt-1
    And the response field "sourcePayload.order_ref" is ord-7-line-1
    And the response field "auditTrail.0.action" is OPENED
    And the response field "auditTrail.0.actor" is system
    And the response has no field "assignee"
    And the response has no field "resolutionCode"
    And the response has no field "rootCause"
    And the response has no field "note"
    And the response has no field "resolvedAt"

  Scenario: The sweeper's escalation is visible on the case as a flag and an audit entry
    Given a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-1 and payload {"task_id":"task-8a1f"} arrives
    When time advances 31 minutes
    And the SLA sweeper runs
    And a GET request is sent to "/exceptions/case-0001"
    Then the response field "escalated" is true
    And the response field "status" is OPEN
    And the response field "auditTrail.1.action" is ESCALATED
    And the response field "auditTrail.1.actor" is system
