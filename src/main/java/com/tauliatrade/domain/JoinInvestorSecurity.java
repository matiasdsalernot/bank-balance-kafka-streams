package com.tauliatrade.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Objects;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class JoinInvestorSecurity {

    private Security security;
    private Investor investor;

    public void updateInvestor(JoinInvestorSecurity investorAndSecurity) {
        if (Objects.isNull(this.investor)) this.investor = investorAndSecurity.getInvestor();
        this.investor.calculateFutureLimit(investorAndSecurity);
    }
}
