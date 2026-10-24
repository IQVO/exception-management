# carton_discrepancy.feature — inbound-receiving's CartonDiscrepancyRaised
# (contract R12, ADR 0006): one routed case per (receipt, sscc|"-", kind),
# idempotent by CloudEvents id AND by that natural key.
Feature: Carton discrepancies open routed cases

  Background:
    Given the clock is 2026-10-24T09:00:00Z
    And a policy is declared for source type com.warehouse.wms.inbound-receiving.receipt.CartonDiscrepancyRaised#problem-solve with severity HIGH queue problem-solve and sla 60 minutes
    And a policy is declared for source type com.warehouse.wms.inbound-receiving.receipt.CartonDiscrepancyRaised#receiving-lead with severity LOW queue receiving-lead and sla 480 minutes

  Scenario Outline: Each kind routes to its queue
    When a carton discrepancy of kind <kind> for receipt rcpt-1 and sscc 340123450000000013 with event id evt-1 arrives
    Then a new case is opened
    And the case queue is <queue>
    And the case severity is <severity>
    And the case occurrence is 1
    And the published events are ExceptionOpened

    Examples:
      | kind                        | queue          | severity |
      | MissingCarton               | problem-solve  | HIGH     |
      | DuplicateSscc               | problem-solve  | HIGH     |
      | UnexpectedCarton            | problem-solve  | HIGH     |
      | CaseQtyMismatch             | problem-solve  | HIGH     |
      | MixedCaseDeclaredAsCasePack | problem-solve  | HIGH     |
      | MixedCartonContentMismatch  | problem-solve  | HIGH     |
      | UnknownGtin                 | problem-solve  | HIGH     |
      | SomeFutureKind              | problem-solve  | HIGH     |
      | UnlabelledCarton            | receiving-lead | LOW      |
      | CartonOverLimit             | receiving-lead | LOW      |

  Scenario: The same receipt, carton and kind raised again touches the case
    When a carton discrepancy of kind MissingCarton for receipt rcpt-1 and sscc 340123450000000013 with event id evt-1 arrives
    And a carton discrepancy of kind MissingCarton for receipt rcpt-1 and sscc 340123450000000013 with event id evt-2 arrives
    Then no new case is opened
    And the case occurrence is 2

  Scenario: A replayed CloudEvents id changes nothing
    When a carton discrepancy of kind MissingCarton for receipt rcpt-1 and sscc 340123450000000013 with event id evt-1 arrives
    And a carton discrepancy of kind MissingCarton for receipt rcpt-1 and sscc 340123450000000013 with event id evt-1 arrives
    Then no new case is opened
    And the case occurrence is 1

  Scenario: A discrepancy without a carton keys on a dash
    When a carton discrepancy of kind UnknownGtin for receipt rcpt-1 and no sscc with event id evt-1 arrives
    Then a new case is opened
    And the case queue is problem-solve
