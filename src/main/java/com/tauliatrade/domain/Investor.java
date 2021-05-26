package com.tauliatrade.domain;

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
    private Long escrow = 0L;
    @Builder.Default
    private Long invested = 0L;

    public void calculateLimit(JoinInvestorSecurity value) {
        this.limit = value.getInvestor().getLimit();
        this.invested += value.getSecurity().getAmount();
        this.limit -= this.invested;
        this.escrow = value.getInvestor().getEscrow();
        this.escrow -= value.getSecurity().getAmount();
    }

    public void calculateFutureLimit(JoinInvestorSecurity investorAndSecurity) {
        this.limit -= investorAndSecurity.getSecurity().getAmount();
        this.escrow += investorAndSecurity.getSecurity().getAmount();
    }

    public void unescrow(JoinInvestorSecurity investorAndSecurity) {
        this.limit = investorAndSecurity.getInvestor().getLimit();
        this.escrow = investorAndSecurity.getInvestor().getEscrow();
        this.limit += investorAndSecurity.getSecurity().getAmount();
        this.escrow -= investorAndSecurity.getSecurity().getAmount();
    }
}
