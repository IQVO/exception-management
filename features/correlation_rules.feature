# correlation_rules.feature — documentation-derived rules of the declarative
# auto-resolve (use-cases #2 AutoResolveOnCorrelation, business-context
# "Auto-resolve (policy rule, consumer-driven)", aggregate-design-canvas
# ExceptionPolicy "Matches", troubleshooting "auto-resolve did not fire"):
# ORDER-level match through the work-unit formula, the window runs from the
# case's LAST occurrence with an inclusive edge, a mismatch is a silent no-op,
# the rule is data of the policy and the actor of the close is the policy.
Feature: Auto-resolve correlation rules

  Background:
    Given the clock is 2026-10-21T09:00:00Z
    And a policy is declared for source type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed with severity HIGH queue ps-main and sla 60 minutes and auto-resolve on com.warehouse.wes.order-management.order.OrderRepromised within 10 minutes

  # --- which cases an OrderRepromised closes -----------------------------------

  Scenario Outline: The correlation is order-level: the line number of the work unit is ignored
    Given a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit <workUnit> at site SIM1 with event id evt-1 and payload {"task_id":"task-8a1f"} arrives
    When an OrderRepromised fact for order ord-7 with event id evt-9 arrives
    Then the correlation closes 1 case
    And the case status is RESOLVED

    Examples:
      | workUnit     |
      | ord-7-line-1 |
      | ord-7-line-2 |
      | ord-7-line-12 |

  Scenario: One repromise closes every open case of that order and leaves other orders open
    Given a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-1 and payload {"task_id":"task-1"} arrives
    And a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-2 at site SIM1 with event id evt-2 and payload {"task_id":"task-2"} arrives
    And a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-8-line-1 at site SIM1 with event id evt-3 and payload {"task_id":"task-3"} arrives
    When an OrderRepromised fact for order ord-7 with event id evt-9 arrives
    Then the correlation closes 2 cases
    And the published events are ExceptionOpened, ExceptionOpened, ExceptionOpened, ExceptionAutoResolved, ExceptionAutoResolved
    And the open caseload has 1 case
    When I look at case case-0003
    Then the case status is OPEN

  Scenario: An order id that merely starts like the case's order does not match
    Given a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-77-line-1 at site SIM1 with event id evt-1 and payload {"task_id":"task-8a1f"} arrives
    When an OrderRepromised fact for order ord-7 with event id evt-9 arrives
    Then the correlation closes 0 cases
    And the case status is OPEN

  Scenario Outline: The window runs from the LAST occurrence of the case, edge inclusive
    Given a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-1 and payload {"task_id":"task-8a1f"} arrives
    When time advances 8 minutes
    And a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-2 and payload {"task_id":"task-8a1f"} arrives
    And time advances <minutes> minutes
    And an OrderRepromised fact for order ord-7 with event id evt-9 arrives
    Then the correlation closes <closed> cases

    Examples:
      | minutes | closed |
      | 7       | 1      |
      | 10      | 1      |
      | 11      | 0      |

  # --- which cases it must NOT close -------------------------------------------

  Scenario Outline: A case whose subject is not a work-unit reference is never closed by order correlation
    Given a policy is declared for source type com.warehouse.wes.work-planning.workpool.BacklogThresholdBreached with severity MEDIUM queue planning and sla 120 minutes and auto-resolve on com.warehouse.wes.order-management.order.OrderRepromised within 10 minutes
    And a fact of type com.warehouse.wes.work-planning.workpool.BacklogThresholdBreached for work unit <subject> at site SIM1 with event id evt-1 and payload {"path_id":"path-17"} arrives
    When an OrderRepromised fact for order <order> with event id evt-9 arrives
    Then the correlation closes 0 cases
    And the case status is OPEN

    Examples:
      | subject       | order  |
      | path-17       | path-17 |
      | ord-7         | ord-7   |
      | ord-7-line-x  | ord-7   |

  Scenario: A policy without an auto-resolve rule never closes its cases
    Given a policy is declared for source type com.warehouse.wes.work-planning.workpool.BacklogThresholdBreached with severity MEDIUM queue planning and sla 120 minutes
    And a fact of type com.warehouse.wes.work-planning.workpool.BacklogThresholdBreached for work unit ord-7-line-1 at site SIM1 with event id evt-1 and payload {"path_id":"path-17"} arrives
    When an OrderRepromised fact for order ord-7 with event id evt-9 arrives
    Then the correlation closes 0 cases
    And the case status is OPEN
    And no auto-resolution event is published

  Scenario: A rule waiting for another correlated type ignores an OrderRepromised
    Given a policy is declared for source type com.warehouse.wes.work-planning.workpool.BacklogThresholdBreached with severity MEDIUM queue planning and sla 120 minutes and auto-resolve on com.warehouse.wes.order-management.order.OrderCancelled within 10 minutes
    And a fact of type com.warehouse.wes.work-planning.workpool.BacklogThresholdBreached for work unit ord-7-line-1 at site SIM1 with event id evt-1 and payload {"path_id":"path-17"} arrives
    When an OrderRepromised fact for order ord-7 with event id evt-9 arrives
    Then the correlation closes 0 cases
    And the case status is OPEN

  Scenario: A case already resolved by an operator is left as the operator closed it
    Given a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-1 and payload {"task_id":"task-8a1f"} arrives
    And operator op-7 resolves the case with code RELOCATED and root cause bin-mismatch
    When an OrderRepromised fact for order ord-7 with event id evt-9 arrives
    Then the correlation closes 0 cases
    And the case resolution code is RELOCATED
    And no auto-resolution event is published

  # --- what the close records ---------------------------------------------------

  Scenario Outline: An unresolved case of any status is closed by the policy, which becomes the actor
    Given a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-1 and payload {"task_id":"task-8a1f"} arrives
    And the case has been brought to status <status>
    When an OrderRepromised fact for order ord-7 with event id evt-9 arrives
    Then the correlation closes 1 case
    And the case status is RESOLVED
    And the case resolution code is AUTO_RESOLVED
    And the case root cause is com.warehouse.wes.order-management.order.OrderRepromised
    And the audit entry AUTO_RESOLVED records actor com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed->com.warehouse.wes.order-management.order.OrderRepromised
    And the last ExceptionAutoResolved event has CorrelatedEventID evt-9
    And the last ExceptionAutoResolved event has CorrelatedType com.warehouse.wes.order-management.order.OrderRepromised

    Examples:
      | status   |
      | OPEN     |
      | TRIAGED  |
      | ASSIGNED |

  Scenario: An auto-resolved case is terminal for the operators too
    Given a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-1 and payload {"task_id":"task-8a1f"} arrives
    And an OrderRepromised fact for order ord-7 with event id evt-9 arrives
    When a POST request is sent to "/exceptions/case-0001/resolutions" with key "k-res" and the JSON body {"resolutionCode":"DONE","rootCause":"HANDLED"}
    Then the response status is 409
    And the response problem is "case-already-resolved"
    When a POST request is sent to "/exceptions/case-0001/assignments" with key "k-asg" and the JSON body {"assignee":"ana"}
    Then the response status is 409
    And the audit trail is OPENED, AUTO_RESOLVED

  Scenario: The auto-resolved case is readable with its rule evidence
    Given a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-1 and payload {"task_id":"task-8a1f"} arrives
    When a GET request is sent to "/exceptions/case-0001"
    Then the response has no field "autoResolveRuleId"
    And the response has no field "resolutionCode"
    When time advances 4 minutes
    And an OrderRepromised fact for order ord-7 with event id evt-9 arrives
    And a GET request is sent to "/exceptions/case-0001"
    Then the response field "status" is RESOLVED
    And the response field "resolutionCode" is AUTO_RESOLVED
    And the response field "autoResolveRuleId" is com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed->com.warehouse.wes.order-management.order.OrderRepromised
    And the response field "resolvedAt" is 2026-10-21T09:04:00Z
    And the response field "note" is null
    And the response field "auditTrail.1.action" is AUTO_RESOLVED
    And the response field "auditTrail.1.actor" is com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed->com.warehouse.wes.order-management.order.OrderRepromised

  # --- the correlation consumer: claim and close in one unit of work -----------

  Scenario: A redelivered OrderRepromised is a duplicate that changes nothing
    Given a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-1 and payload {"task_id":"task-8a1f"} arrives
    When an OrderRepromised fact for order ord-7 with event id evt-9 is consumed
    Then the consume outcome is applied
    And the correlation closes 1 case
    When an OrderRepromised fact for order ord-7 with event id evt-9 is consumed
    Then the consume outcome is duplicate
    And the correlation closes 0 cases
    And the published events are ExceptionOpened, ExceptionAutoResolved

  Scenario: A repromise that matches nothing is a normal no-match, not an error
    Given a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-1 and payload {"task_id":"task-8a1f"} arrives
    When an OrderRepromised fact for order ord-8 with event id evt-9 is consumed
    Then the consume outcome is no-match
    And the correlation closes 0 cases
    And the case status is OPEN

  Scenario: A handled no-match still claims the event id
    Given a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-1 and payload {"task_id":"task-8a1f"} arrives
    When an OrderRepromised fact for order ord-8 with event id evt-9 is consumed
    And an OrderRepromised fact for order ord-7 with event id evt-9 is consumed
    Then the consume outcome is duplicate
    And the case status is OPEN

  Scenario: An OrderRepromised without a CloudEvents id is refused before any claim
    Given a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-1 and payload {"task_id":"task-8a1f"} arrives
    When an OrderRepromised fact for order ord-7 with event id  is consumed
    Then the command fails with ErrNoEventID
    And the case status is OPEN
    And no auto-resolution event is published
