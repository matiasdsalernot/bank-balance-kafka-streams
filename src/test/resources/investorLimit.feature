Feature: I want to calculate investors limits

  Scenario: I submit a new Security Creation Request and I get new investor limit
    Given the investor "BBVA" with id 1 and a limit of 10000
    When I send the security creation request with id 1 and amount of 100 to the topic "security-creation-request"
    Then the investor's new limit of 9900 is published in the topic "investor-limits"