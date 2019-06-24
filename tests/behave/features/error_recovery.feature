Feature: Error Recovery

  Scenario: recovering connection after connection loss
    Given a psycopg2 db config
    And a pgware instance
      When we open a connection
      And we fetchval query "SELECT 1"
        Then no error is raised
        And we obtain "1"
      When we sabotage the psycopg2 connection
      And we fetchval query "SELECT 2"
        Then no error is raised
        And we obtain "2"
