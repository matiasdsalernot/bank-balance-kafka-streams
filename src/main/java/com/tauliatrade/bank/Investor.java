package com.tauliatrade.bank;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Investor {

    private String id;
    private String name;
    private Long limit;
    @Builder.Default
    private Long invested = 0L;
    @Builder.Default
    private InvestorState state = InvestorState.CREATED;

    public void calculateLimit(BankBalanceApp.JoinedInvestorSecurityCreationRequest investorSecurityCreationRequest) {
        if (state == InvestorState.CREATED) {
            this.limit = investorSecurityCreationRequest.getInvestor().getLimit();
            this.state = InvestorState.AGGREGATED;
        }
        if(investorSecurityCreationRequest.getInvestor().getState() == InvestorState.UPDATE) {
            this.limit = investorSecurityCreationRequest.getInvestor().getLimit() - invested;
        }
        this.invested += investorSecurityCreationRequest.getSecurityCreationRequest().getAmount();
        this.limit -= investorSecurityCreationRequest.getSecurityCreationRequest().getAmount();
    }

    public enum InvestorState {
        CREATED, UPDATE, AGGREGATED
    }
}
