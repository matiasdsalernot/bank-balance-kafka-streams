package com.tauliatrade.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Security {

    private String id;
    private String investorId;
    private Long amount;

}
