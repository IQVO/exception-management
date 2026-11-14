# policy_rules.feature — documentation-derived rules of the versioned handling
# policy (use-cases #7 DeclareExceptionPolicy and #10 ListExceptionPolicies,
# aggregate-design-canvas ExceptionPolicy "Invariants"/"Lifecycle": severity
# valid, queue non-blank, SLAMinutes >= 1, WithinMinutes >= 1 when the rule
# exists, versions append-only, latest version per source type listed in
# source-type order, one keyset page at a time).
Feature: Exception policy rules

  Background:
    Given the clock is 2026-10-21T09:00:00Z

  Scenario Outline: Every severity of the language is accepted
    When a PUT request is sent to "/exception-policies/com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed" with key "pol-1" and the JSON body {"severity":"<severity>","queue":"ps-main","slaMinutes":30}
    Then the response status is 201
    And the response field "severity" is <severity>
    And the response field "version" is 1

    Examples:
      | severity |
      | LOW      |
      | MEDIUM   |
      | HIGH     |
      | CRITICAL |

  Scenario Outline: A severity outside the language is refused
    When a PUT request is sent to "/exception-policies/com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed" with key "pol-1" and the JSON body {"severity":"<severity>","queue":"ps-main","slaMinutes":30}
    Then the response status is 400
    And the response problem is "invalid-severity"

    Examples:
      | severity |
      | high     |
      | Critical |
      |          |

  Scenario: An SLA of exactly one minute is the smallest accepted
    When a PUT request is sent to "/exception-policies/com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed" with key "pol-1" and the JSON body {"severity":"HIGH","queue":"ps-main","slaMinutes":1}
    Then the response status is 201
    And the response field "slaMinutes" is 1

  Scenario Outline: An SLA below one minute, or none, is refused
    When a PUT request is sent to "/exception-policies/com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed" with key "pol-1" and the JSON body <body>
    Then the response status is 400
    And the response problem is "invalid-policy"

    Examples:
      | body                                                |
      | {"severity":"HIGH","queue":"ps-main","slaMinutes":0}  |
      | {"severity":"HIGH","queue":"ps-main","slaMinutes":-5} |
      | {"severity":"HIGH","queue":"ps-main"}                 |

  Scenario: A queue of exactly 64 characters is accepted and 65 is refused
    When a PUT request is sent to "/exception-policies/com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed" with key "pol-1" and the JSON body {"severity":"HIGH","queue":"qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq","slaMinutes":30}
    Then the response status is 201
    When a PUT request is sent to "/exception-policies/com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed" with key "pol-2" and the JSON body {"severity":"HIGH","queue":"qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq","slaMinutes":30}
    Then the response status is 400
    And the response problem is "invalid-queue"

  Scenario: A blank queue is refused and no policy exists afterwards
    When a PUT request is sent to "/exception-policies/com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed" with key "pol-1" and the JSON body {"severity":"HIGH","queue":"   ","slaMinutes":30}
    Then the response status is 400
    And the response problem is "invalid-queue"
    When a GET request is sent to "/exception-policies"
    Then the response list "items" has 0 items

  # --- the auto-resolve rule ------------------------------------------------------

  Scenario: A declared auto-resolve rule is stored as data and read back
    When a PUT request is sent to "/exception-policies/com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed" with key "pol-1" and the JSON body {"severity":"HIGH","queue":"ps-main","slaMinutes":60,"autoResolve":{"onType":"com.warehouse.wes.order-management.order.OrderRepromised","withinMinutes":1}}
    Then the response status is 201
    And the response field "autoResolve.onType" is com.warehouse.wes.order-management.order.OrderRepromised
    And the response field "autoResolve.withinMinutes" is 1

  Scenario Outline: A broken auto-resolve rule is an invalid policy
    When a PUT request is sent to "/exception-policies/com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed" with key "pol-1" and the JSON body {"severity":"HIGH","queue":"ps-main","slaMinutes":60,"autoResolve":{"onType":"<onType>","withinMinutes":<within>}}
    Then the response status is 400
    And the response problem is "invalid-policy"

    Examples:
      | onType                                                   | within |
      | com.warehouse.wes.order-management.order.OrderRepromised | 0      |
      | com.warehouse.wes.order-management.order.OrderRepromised | -3     |
      | OrderRepromised                                          | 10     |

  Scenario Outline: The policy aggregate refuses what the contract refuses, even driven directly
    When a policy is declared for source type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed with severity HIGH queue ps-main and sla <sla> minutes and auto-resolve on <onType> within <within> minutes
    Then the policy declaration fails with <error>
    When a GET request is sent to "/exception-policies"
    Then the response list "items" has 0 items

    Examples:
      | sla | onType | within | error |
      | 30  | com.warehouse.wes.order-management.order.OrderRepromised | 0      | ErrInvalidSLA |
      | 0   | com.warehouse.wes.order-management.order.OrderRepromised | 10     | ErrInvalidSLA |
      | 30  | OrderRepromised | 10 | ErrInvalidSourceType |

  Scenario: Redeclaring without a rule removes the rule from the latest version only
    Given a policy is declared for source type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed with severity HIGH queue ps-main and sla 60 minutes and auto-resolve on com.warehouse.wes.order-management.order.OrderRepromised within 10 minutes
    When a PUT request is sent to "/exception-policies/com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed" with key "pol-2" and the JSON body {"severity":"HIGH","queue":"ps-main","slaMinutes":60}
    Then the response status is 201
    And the response field "version" is 2
    And the response field "autoResolve" is null
    And the policy for com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed is at version 2

  # --- versions are append-only --------------------------------------------------

  Scenario: Every redeclaration appends the next version and the listing shows only the latest
    Given a policy is declared for source type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed with severity HIGH queue ps-main and sla 30 minutes
    And a policy is declared for source type com.warehouse.wes.work-planning.workpool.BacklogThresholdBreached with severity MEDIUM queue planning and sla 120 minutes
    When a PUT request is sent to "/exception-policies/com.warehouse.wes.network-fulfillment.networkorder.AcknowledgementDeadlineAtRisk" with key "pol-1" and the JSON body {"severity":"MEDIUM","queue":"ack-desk","slaMinutes":60}
    Then the response status is 201
    And the response field "version" is 1
    When a PUT request is sent to "/exception-policies/com.warehouse.wes.network-fulfillment.networkorder.AcknowledgementDeadlineAtRisk" with key "pol-2" and the JSON body {"severity":"HIGH","queue":"ack-night","slaMinutes":20}
    Then the response status is 201
    And the response field "version" is 2
    And the policy declarations published for com.warehouse.wes.network-fulfillment.networkorder.AcknowledgementDeadlineAtRisk are versions 1, 2
    When a GET request is sent to "/exception-policies"
    Then the response list "items" has 3 items
    And the response field "items.1.sourceType" is com.warehouse.wes.network-fulfillment.networkorder.AcknowledgementDeadlineAtRisk
    And the response field "items.1.version" is 2
    And the response field "items.1.queue" is ack-night

  Scenario: The policy listing is in source-type order and pages by keyset
    Given a policy is declared for source type com.warehouse.wes.work-planning.workpool.BacklogThresholdBreached with severity MEDIUM queue planning and sla 120 minutes
    And a policy is declared for source type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed with severity HIGH queue ps-main and sla 30 minutes
    And a policy is declared for source type com.warehouse.wes.network-fulfillment.networkorder.AcknowledgementDeadlineAtRisk with severity MEDIUM queue ack-desk and sla 60 minutes
    When a GET request is sent to "/exception-policies?limit=2"
    Then the response status is 200
    And the response list "items" has 2 items
    And the response field "items.0.sourceType" is com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed
    And the response field "items.1.sourceType" is com.warehouse.wes.network-fulfillment.networkorder.AcknowledgementDeadlineAtRisk
    When a GET request is sent to "/exception-policies?limit=2" with the cursor of the previous page
    Then the response status is 200
    And the response list "items" has 1 items
    And the response field "items.0.sourceType" is com.warehouse.wes.work-planning.workpool.BacklogThresholdBreached
    And the response has no field "nextCursor"

  Scenario: A policy page that exactly fills the limit has no next cursor
    Given a policy is declared for source type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed with severity HIGH queue ps-main and sla 30 minutes
    And a policy is declared for source type com.warehouse.wes.network-fulfillment.networkorder.AcknowledgementDeadlineAtRisk with severity MEDIUM queue ack-desk and sla 60 minutes
    When a GET request is sent to "/exception-policies?limit=2"
    Then the response list "items" has 2 items
    And the response has no field "nextCursor"

  # --- strict bodies ---------------------------------------------------------------

  Scenario Outline: A policy body outside the contract is a malformed request
    When a PUT request is sent to "/exception-policies/com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed" with key "pol-1" and the JSON body <body>
    Then the response status is 400
    And the response problem is "malformed-request-body"

    Examples:
      | body                                                                       |
      | {"severity":"HIGH","queue":"ps-main","slaMinutes":30,"priority":1}        |
      | {"severity":"HIGH","queue":"ps-main","slaMinutes":30} {"severity":"LOW"}  |
      | {"severity":"HIGH","queue":"ps-main","slaMinutes":"thirty"}                |

  Scenario: A NUL character in a body is a malformed request before anything is recorded
    When a PUT request is sent to "/exception-policies/com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed" with key "pol-1" and the JSON body {"severity":"HIGH","queue":"ps\u0000main","slaMinutes":30}
    Then the response status is 400
    And the response problem is "malformed-request-body"
    When a GET request is sent to "/exception-policies"
    Then the response list "items" has 0 items

  Scenario: A raw NUL byte in a body is a malformed request too
    When a PUT request is sent to "/exception-policies/com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed" with key "pol-1" and a body holding a raw NUL byte
    Then the response status is 400
    And the response problem is "malformed-request-body"
    When a GET request is sent to "/exception-policies"
    Then the response list "items" has 0 items

  Scenario: A NUL in the query string is malformed and a NUL in the path names no resource
    When a GET request is sent to "/exceptions?queue=ps%00main"
    Then the response status is 400
    And the response problem is "malformed-request-body"
    When a GET request is sent to "/exceptions/case-%00x"
    Then the response status is 404
    And the response problem is "not-found"
