Feature: I want to publish new investor limits everytime there is a new security creation request
  Background:
    Given I have a kafka instance running

  Scenario: I publish a security creation request and a new limit is created
    Given the investor "BBVA" with Id "1", and limit 10000 GBP
    When I send a security creation request with an amount of 100 GBP to the topic "sec-creation-request"
    Then I receive a new updated investor limit in the topic "investor-limits"