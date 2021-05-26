Feature: I want to be able to create securities

  Scenario: I create a security successfully updating the investor's limit
    Given the investor "BBVA" with id 1 and a limit of 10000
    When I send the security creation request with id 1 and amount of 100 to the topic "security-creation-request"
    Then a security is created in the "securities" topic
    And the investor's new limit of 9900 is published in the topic "investor-limits"

  Scenario: I can't create a security that exceeds the investor's limit
    Given the investor "Santander" with id 1 and a limit of 1000
    When I send the security creation request with id 1 and amount of 1001 to the topic "security-creation-request"
    Then the request is rejected and published in the topic "rejected-security-creation-requests"