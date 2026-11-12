# rest_edge.feature — the HTTP edge of the API (wave 2): every operation of
# apis/openapi.yaml through the real chi router, the RFC 7807 problem
# catalogue and the fleet Idempotency-Key middleware. Cases open only from
# consumed facts (ADR 0001), so POST /exceptions is a permanent 405.
Feature: The REST edge of exception management

  Background:
    Given the clock is 2026-10-21T09:00:00Z

  Scenario: A policy is declared and listed over the API
    When a PUT request is sent to "/exception-policies/com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed" with key "pol-1" and the JSON body {"severity":"HIGH","queue":"ps-main","slaMinutes":30}
    Then the response status is 201
    And the response field "sourceType" is com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed
    And the response field "severity" is HIGH
    And the response field "slaMinutes" is 30
    When a GET request is sent to "/exception-policies"
    Then the response status is 200
    And the response list "items" has 1 items
    And the response field "items.0.queue" is ps-main

  Scenario: A broken policy declaration is refused
    When a PUT request is sent to "/exception-policies/com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed" with key "pol-1" and the JSON body {"severity":"HIGH","queue":"ps-main","slaMinutes":0}
    Then the response status is 400
    And the response problem is "invalid-policy"
    When a PUT request is sent to "/exception-policies/not-a-type" with key "pol-2" and the JSON body {"severity":"HIGH","queue":"ps-main","slaMinutes":30}
    Then the response status is 400
    And the response problem is "invalid-source-type"
    When a PUT request is sent to "/exception-policies/com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed" with key "pol-3" and the JSON body {"severity":"URGENT","queue":"ps-main","slaMinutes":30}
    Then the response status is 400
    And the response problem is "invalid-severity"

  Scenario: A write without an Idempotency-Key is refused before anything runs
    When a PUT request is sent to "/exception-policies/com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed" with the JSON body {"severity":"HIGH","queue":"ps-main","slaMinutes":30}
    Then the response status is 400
    And the response problem is "idempotency-key-required"

  Scenario: The case lifecycle over the API
    Given a policy is declared for source type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed with severity HIGH queue ps-main and sla 30 minutes
    And a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-1 and payload {"task_id":"task-8a1f"} arrives
    When a GET request is sent to "/exceptions/case-0001"
    Then the response status is 200
    And the response field "status" is OPEN
    And the response field "severity" is HIGH
    And the response field "occurrenceCount" is 1
    When a POST request is sent to "/exceptions/case-0001/triage-decisions" with key "tri-1" as operator-1 and the JSON body {"severity":"CRITICAL","queue":"ps-lead"}
    Then the response status is 200
    And the response field "status" is TRIAGED
    And the response field "severity" is CRITICAL
    When a POST request is sent to "/exceptions/case-0001/assignments" with key "asg-1" as operator-1 and the JSON body {"assignee":"ana"}
    Then the response status is 200
    And the response field "status" is ASSIGNED
    And the response field "assignee" is ana
    When a POST request is sent to "/exceptions/case-0001/resolutions" with key "res-1" as operator-1 and the JSON body {"resolutionCode":"FIXED","rootCause":"MISCONFIGURED_CPT"}
    Then the response status is 200
    And the response field "status" is RESOLVED
    And the response field "resolutionCode" is FIXED

  Scenario: A resolved case refuses further transitions
    Given a policy is declared for source type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed with severity HIGH queue ps-main and sla 30 minutes
    And a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-1 and payload {"task_id":"task-8a1f"} arrives
    And operator op resolves the case with code FIXED and root cause done
    When a POST request is sent to "/exceptions/case-0001/triage-decisions" with key "tri-2" as operator-1 and the JSON body {"severity":"LOW","queue":"ps-main"}
    Then the response status is 409
    And the response problem is "case-already-resolved"

  Scenario: An unknown case is a typed 404
    When a GET request is sent to "/exceptions/case-9999"
    Then the response status is 404
    And the response problem is "case-not-found"
    When a POST request is sent to "/exceptions/case-9999/assignments" with key "asg-9" as operator-1 and the JSON body {"assignee":"ana"}
    Then the response status is 404
    And the response problem is "case-not-found"

  Scenario: Command validation is typed
    Given a policy is declared for source type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed with severity HIGH queue ps-main and sla 30 minutes
    And a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-1 and payload {"task_id":"task-8a1f"} arrives
    When a POST request is sent to "/exceptions/case-0001/triage-decisions" with key "tri-1" as operator-1 and the JSON body {"severity":"CRITICAL","queue":" "}
    Then the response status is 400
    And the response problem is "invalid-queue"
    When a POST request is sent to "/exceptions/case-0001/assignments" with key "asg-1" as operator-1 and the JSON body {"assignee":""}
    Then the response status is 400
    And the response problem is "invalid-assignee"
    When a POST request is sent to "/exceptions/case-0001/resolutions" with key "res-1" as operator-1 and the JSON body {"resolutionCode":"","rootCause":"x"}
    Then the response status is 400
    And the response problem is "invalid-resolution"
    When a POST request is sent to "/exceptions/case-0001/triage-decisions" with key "tri-2" and the JSON body {"severity":"LOW","queue":"ps-main"}
    Then the response status is 200
    And the response field "auditTrail.1.actor" is system

  Scenario: POST /exceptions is a permanent 405 with the reason
    When a POST request is sent to "/exceptions" with key "mk-1" and the JSON body {"sourceType":"com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed"}
    Then the response status is 405
    And the response header "Allow" is "GET"

  Scenario: An idempotent replay answers the recorded response and changes nothing
    When a PUT request is sent to "/exception-policies/com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed" with key "pol-1" and the JSON body {"severity":"HIGH","queue":"ps-main","slaMinutes":30}
    Then the response status is 201
    When a PUT request is sent to "/exception-policies/com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed" with key "pol-1" and the JSON body {"severity":"HIGH","queue":"ps-main","slaMinutes":30}
    Then the response status is 201
    And the response header "Idempotency-Replayed" is "true"
    When a PUT request is sent to "/exception-policies/com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed" with key "pol-1" and the JSON body {"severity":"LOW","queue":"ps-main","slaMinutes":30}
    Then the response status is 409
    And the response problem is "idempotency-key-reused"

  Scenario: A malformed body is refused
    When a PUT request is sent to "/exception-policies/com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed" with key "pol-1" and the JSON body {"severity":"HIGH",
    Then the response status is 400
    And the response problem is "malformed-request-body"

  Scenario: The case list pages by keyset and validates its parameters
    Given a policy is declared for source type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed with severity HIGH queue ps-main and sla 30 minutes
    And a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-1 and payload {"n":1} arrives
    And a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-2 at site SIM1 with event id evt-2 and payload {"n":2} arrives
    And a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-3 at site SIM1 with event id evt-3 and payload {"n":3} arrives
    When a GET request is sent to "/exceptions?limit=2"
    Then the response status is 200
    And the response list "items" has 2 items
    And the response field "items.0.caseId" is case-0003
    And the response field "items.1.caseId" is case-0002
    When a GET request is sent to "/exceptions?limit=0"
    Then the response status is 400
    And the response problem is "invalid-limit"
    When a GET request is sent to "/exceptions?cursor=not-a-cursor"
    Then the response status is 400
    And the response problem is "invalid-cursor"
    When a GET request is sent to "/exceptions?bogus=1"
    Then the response status is 400
    And the response problem is "unknown-query-parameter"
    When a GET request is sent to "/exceptions?status=FROZEN"
    Then the response status is 400
    And the response problem is "invalid-status"

  Scenario: The case list filters by status
    Given a policy is declared for source type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed with severity HIGH queue ps-main and sla 30 minutes
    And a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-1 and payload {"n":1} arrives
    And a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-2 at site SIM1 with event id evt-2 and payload {"n":2} arrives
    And operator op resolves the case with code FIXED and root cause done
    When a GET request is sent to "/exceptions?status=RESOLVED"
    Then the response status is 200
    And the response list "items" has 1 items
    And the response field "items.0.caseId" is case-0002
    When a GET request is sent to "/exceptions?status=OPEN"
    Then the response status is 200
    And the response list "items" has 1 items
    And the response field "items.0.caseId" is case-0001
