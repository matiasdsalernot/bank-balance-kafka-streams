package com.tauliatrade.securities.stepdefs;

import io.cucumber.java.en.And;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

public class SecurityCreationStepDefs {
    @Given("the investor {string} with id {int} and a limit of {int}")
    public void theInvestorWithIdAndALimitOf(String arg0, int arg1, int arg2) {
    }

    @When("I send the security creation request with id {int} and amount of {int} to the topic {string}")
    public void iSendTheSecurityCreationRequestWithIdAndAmountOfToTheTopic(int arg0, int arg1, String arg2) {
    }

    @Then("a security is created in the {string} topic")
    public void aSecurityIsCreatedInTheTopic(String arg0) {
    }

    @And("the investor's new limit of {int} is published in the topic {string}")
    public void theInvestorSNewLimitOfIsPublishedInTheTopic(int arg0, String arg1) {
    }

    @Then("the request is rejected and published in the topic {string}")
    public void theRequestIsRejectedAndPublishedInTheTopic(String arg0) {
    }
}
