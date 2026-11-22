# readiness_rules.feature — documentation-derived rules of the probes
# (troubleshooting "Readiness": /readyz is 503 dependency-unavailable naming
# postgres or kafka while a dependency is down, liveness stays 200 so the pod is
# not killed, only broker reachability counts for Kafka, and the drain gate
# answers {"status":"not_ready"} without dependencies during a rollover).
Feature: Liveness and readiness probes

  Scenario: A healthy service is live and ready
    When a GET request is sent to "/healthz"
    Then the response status is 200
    And the response field "status" is ok
    When a GET request is sent to "/readyz"
    Then the response status is 200
    And the response field "status" is ready

  Scenario: Postgres down makes the service not ready and names postgres, while liveness stays 200
    Given Postgres is unreachable
    When a GET request is sent to "/readyz"
    Then the response status is 503
    And the response problem is "dependency-unavailable"
    And the response field "detail" is not ready: postgres
    When a GET request is sent to "/healthz"
    Then the response status is 200

  Scenario: No Kafka broker makes the service not ready and names kafka
    Given no Kafka broker answers
    When a GET request is sent to "/readyz"
    Then the response status is 503
    And the response problem is "dependency-unavailable"
    And the response field "detail" is not ready: kafka
    When a GET request is sent to "/healthz"
    Then the response status is 200

  Scenario: Both dependencies down are both named
    Given Postgres is unreachable
    And no Kafka broker answers
    When a GET request is sent to "/readyz"
    Then the response status is 503
    And the response field "detail" is not ready: postgres, kafka

  Scenario: The service is ready again as soon as the dependency answers
    Given Postgres is unreachable
    When a GET request is sent to "/readyz"
    Then the response status is 503
    Given Postgres is reachable again
    When a GET request is sent to "/readyz"
    Then the response status is 200
    And the response field "status" is ready

  Scenario: The drain gate answers not_ready without a dependency, and liveness is untouched
    Given the service starts draining for shutdown
    When a GET request is sent to "/readyz"
    Then the response status is 503
    And the response field "status" is not_ready
    And the response has no field "type"
    When a GET request is sent to "/healthz"
    Then the response status is 200
    And the response field "status" is ok
