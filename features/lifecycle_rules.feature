# lifecycle_rules.feature — documentation-derived rules of the operator lifecycle
# (use-cases #4-#6 Triage/Assign/Resolve, aggregate-design-canvas "State
# Transitions" and "Invariants": OPEN -> TRIAGED -> ASSIGNED -> RESOLVED, one
# direction, any order of triage/assign while unresolved, RESOLVED terminal,
# exactly one audit entry per accepted transition, troubleshooting "REST
# errors"). Each scenario names ONE rule.
Feature: Lifecycle rules of an exception case

  Background:
    Given the clock is 2026-10-21T09:00:00Z
    And a policy is declared for source type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed with severity HIGH queue ps-main and sla 30 minutes
    And a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-1 and payload {"task_id":"task-8a1f"} arrives

  # --- the edges of the status machine ----------------------------------------

  Scenario: An owner may take an OPEN case without a triage first
    When operator op-9 assigns the case to ana
    Then the case status is ASSIGNED
    And the case severity is HIGH
    And the case queue is ps-main
    And the audit trail is OPENED, ASSIGNED
    And the published events are ExceptionOpened, ExceptionAssigned

  Scenario: A triage never moves an ASSIGNED case backwards
    Given operator op-9 assigns the case to ana
    When operator op-9 triages the case to severity CRITICAL queue ps-urgent
    Then the case status is ASSIGNED
    And the case assignee is ana
    And the case severity is CRITICAL
    And the case queue is ps-urgent
    And the audit trail is OPENED, ASSIGNED, TRIAGED

  Scenario: Triage may be repeated and the latest severity and queue win
    Given operator op-9 triages the case to severity CRITICAL queue ps-urgent
    When operator op-9 triages the case to severity LOW queue ps-night
    Then the case status is TRIAGED
    And the case severity is LOW
    And the case queue is ps-night
    And the audit trail is OPENED, TRIAGED, TRIAGED
    And the published events are ExceptionOpened, ExceptionTriaged, ExceptionTriaged
    And the last ExceptionTriaged event has Severity LOW
    And the last ExceptionTriaged event has Queue ps-night

  Scenario: Reassignment is allowed while unresolved and records who reassigned
    When a POST request is sent to "/exceptions/case-0001/assignments" with key "asg-1" as op-3 and the JSON body {"assignee":"ana"}
    Then the response status is 200
    When a POST request is sent to "/exceptions/case-0001/assignments" with key "asg-2" as op-4 and the JSON body {"assignee":"bruno"}
    Then the response status is 200
    And the response field "status" is ASSIGNED
    And the response field "assignee" is bruno
    And the response field "auditTrail.1.actor" is op-3
    And the response field "auditTrail.2.actor" is op-4
    And the published events are ExceptionOpened, ExceptionAssigned, ExceptionAssigned
    And the last ExceptionAssigned event has Assignee bruno

  Scenario Outline: A case can be resolved from any unresolved status
    Given the case has been brought to status <status>
    When operator op-7 resolves the case with code RELOCATED and root cause bin-mismatch
    Then the case status is RESOLVED
    And the case resolution code is RELOCATED
    And the last ExceptionResolved event has ResolutionCode RELOCATED

    Examples:
      | status   |
      | OPEN     |
      | TRIAGED  |
      | ASSIGNED |

  # --- RESOLVED is terminal ----------------------------------------------------

  Scenario Outline: A resolved case refuses every further command
    Given operator op-7 resolves the case with code DONE and root cause handled
    When a POST request is sent to "<path>" with key "<key>" and the JSON body <body>
    Then the response status is 409
    And the response problem is "case-already-resolved"
    And the audit trail is OPENED, RESOLVED
    And the published events are ExceptionOpened, ExceptionResolved

    Examples:
      | path                                    | key   | body                                      |
      | /exceptions/case-0001/triage-decisions  | k-tri | {"severity":"LOW","queue":"ps-main"}     |
      | /exceptions/case-0001/assignments       | k-asg | {"assignee":"ana"}                        |
      | /exceptions/case-0001/resolutions       | k-res | {"resolutionCode":"AGAIN","rootCause":"HANDLED_AGAIN"} |

  Scenario: There is no way back: a resolved case cannot be reopened
    Given operator op-7 resolves the case with code DONE and root cause handled
    When a POST request is sent to "/exceptions/case-0001/reopenings" with key "k-reopen" and the JSON body {}
    Then the response status is 404
    And the response problem is "not-found"
    And the case status is RESOLVED
    And the audit trail is OPENED, RESOLVED

  # --- refused commands leave no trace ----------------------------------------

  Scenario: A refused command changes nothing: no audit entry, no event, no version bump
    When a POST request is sent to "/exceptions/case-0001/triage-decisions" with key "k-1" and the JSON body {"severity":"URGENT","queue":"ps-main"}
    Then the response status is 400
    And the response problem is "invalid-severity"
    When a POST request is sent to "/exceptions/case-0001/assignments" with key "k-2" and the JSON body {"assignee":"   "}
    Then the response status is 400
    And the response problem is "invalid-assignee"
    When operator op-7 resolves the case with code RELOCATED and root cause ""
    Then the command fails with ErrBlankRootCause
    And the case status is OPEN
    And the audit trail is OPENED
    And the case version is 1
    And the published events are ExceptionOpened

  Scenario: An assignee of exactly 64 characters is accepted and 65 is refused
    When a POST request is sent to "/exceptions/case-0001/assignments" with key "asg-1" and the JSON body {"assignee":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
    Then the response status is 200
    When a POST request is sent to "/exceptions/case-0001/assignments" with key "asg-2" and the JSON body {"assignee":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
    Then the response status is 400
    And the response problem is "invalid-assignee"
    And the case assignee is aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa

  Scenario Outline: A resolution code and a root cause are codes, not prose
    When a POST request is sent to "/exceptions/case-0001/resolutions" with key "k-1" and the JSON body {"resolutionCode":"<code>","rootCause":"<cause>"}
    Then the response status is 400
    And the response problem is "invalid-resolution"
    And the case status is OPEN

    Examples:
      | code     | cause              |
      | relocated | BIN_MISMATCH       |
      | RELOCATED | bin mismatch       |
      | RELO-CATED | BIN_MISMATCH      |
      | RELOCATED | AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA |

  Scenario: A resolution code and a root cause of exactly 64 characters are accepted
    When a POST request is sent to "/exceptions/case-0001/resolutions" with key "k-1" and the JSON body {"resolutionCode":"RRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR","rootCause":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}
    Then the response status is 200
    And the response field "status" is RESOLVED

  Scenario: A resolution without a note records none, and one with a note keeps it
    When a POST request is sent to "/exceptions/case-0001/resolutions" with key "k-1" as op-7 and the JSON body {"resolutionCode":"RELOCATED","rootCause":"BIN_MISMATCH"}
    Then the response status is 200
    And the response field "note" is null
    And the response field "resolvedAt" is 2026-10-21T09:00:00Z
    And the last ExceptionResolved event has Actor op-7

  Scenario: A resolution note travels with the case and the published fact
    When a POST request is sent to "/exceptions/case-0001/resolutions" with key "k-1" as op-7 and the JSON body {"resolutionCode":"RELOCATED","rootCause":"BIN_MISMATCH","note":"Relabelled bin A-12-03"}
    Then the response status is 200
    And the response field "note" is Relabelled bin A-12-03
    And the last ExceptionResolved event has Note Relabelled bin A-12-03

  # --- the Idempotency-Key on the commands ------------------------------------

  Scenario Outline: Every command needs an Idempotency-Key before anything runs
    When a POST request is sent to "<path>" with the JSON body <body>
    Then the response status is 400
    And the response problem is "idempotency-key-required"
    And the case status is OPEN
    And the published events are ExceptionOpened

    Examples:
      | path                                    | body                                                   |
      | /exceptions/case-0001/triage-decisions  | {"severity":"LOW","queue":"ps-main"}                  |
      | /exceptions/case-0001/assignments       | {"assignee":"ana"}                                     |
      | /exceptions/case-0001/resolutions       | {"resolutionCode":"DONE","rootCause":"HANDLED"}      |

  Scenario: A resolve replayed with its own key answers the stored response and resolves once
    When a POST request is sent to "/exceptions/case-0001/resolutions" with key "res-1" as op-7 and the JSON body {"resolutionCode":"RELOCATED","rootCause":"BIN_MISMATCH"}
    Then the response status is 200
    When a POST request is sent to "/exceptions/case-0001/resolutions" with key "res-1" as op-7 and the JSON body {"resolutionCode":"RELOCATED","rootCause":"BIN_MISMATCH"}
    Then the response status is 200
    And the response header "Idempotency-Replayed" is "true"
    And the published events are ExceptionOpened, ExceptionResolved
    And the audit trail is OPENED, RESOLVED
    When a POST request is sent to "/exceptions/case-0001/resolutions" with key "res-2" as op-7 and the JSON body {"resolutionCode":"RELOCATED","rootCause":"BIN_MISMATCH"}
    Then the response status is 409
    And the response problem is "case-already-resolved"

  # --- the case id bounds -----------------------------------------------------

  Scenario Outline: The case id of the path is bounded by the contract
    When a GET request is sent to "<path>"
    Then the response status is <status>
    And the response problem is "<problem>"

    Examples:
      | path                 | status | problem          |
      | /exceptions/case-    | 400    | invalid-case-id  |
      | /exceptions/ord-7    | 400    | invalid-case-id  |
      | /exceptions/case-1   | 404    | case-not-found   |
      | /exceptions/case-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx | 404 | case-not-found |
      | /exceptions/case-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx | 400 | invalid-case-id |

  # --- versions and published facts -------------------------------------------

  Scenario: The aggregate version grows by one per accepted transition, touches included
    When a GET request is sent to "/exceptions/case-0001"
    Then the response field "version" is 1
    When a fact of type com.warehouse.wes.fulfillment-execution.task.TaskCPTMissed for work unit ord-7-line-1 at site SIM1 with event id evt-2 and payload {"task_id":"task-8a1f"} arrives
    And operator op-9 triages the case to severity CRITICAL queue ps-urgent
    And operator op-9 assigns the case to ana
    Then the case version is 4
    And the last ExceptionAssigned event has Version 4
    When a POST request is sent to "/exceptions/case-0001/triage-decisions" with key "k-1" and the JSON body {"severity":"BAD","queue":"ps-main"}
    Then the response status is 400
    And the case version is 4

  Scenario: The opening fact carries the frozen key, the policy defaults and the SLA deadline
    Then the last ExceptionOpened event has Key.SourceSubject ord-7-line-1
    And the last ExceptionOpened event has Key.SiteID SIM1
    And the last ExceptionOpened event has Severity HIGH
    And the last ExceptionOpened event has Queue ps-main
    And the last ExceptionOpened event has SLADueAt 2026-10-21T09:30:00Z
    And the last ExceptionOpened event has SourceEventID evt-1
    And the last ExceptionOpened event has SourcePayload {"task_id":"task-8a1f"}
    And the last ExceptionOpened event has Version 1
