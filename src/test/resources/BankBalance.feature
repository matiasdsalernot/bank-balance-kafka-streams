Feature: I want to process bank transactions and publish new bank balances

  Scenario: I send a bank transaction and it get's processed
    Given the user with name "John" and bank balance 1000
    When I send a transaction for "John" with the amount of 100 to the topic "bank-transactions"
    Then "John"'s new bank balance of 1100 is published in "bank-balance"

  Scenario: I send a negative bank transaction
    Given the user with name "Peter" and bank balance 1000
    When I send a transaction for "Peter" with the amount of -100 to the topic "bank-transactions"
    Then "Peter"'s new bank balance of 900 is published in "bank-balance"

  Scenario: I send a negative bank transaction
    Given the user with name "Paul" and bank balance 400
    When I send a transaction for "Paul" with the amount of -500 to the topic "bank-transactions"
    Then the transaction is sent to the "rejected-bank-transactions" topic