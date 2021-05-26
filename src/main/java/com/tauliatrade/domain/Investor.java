package com.tauliatrade.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Investor {

    private String id;
    private String name;
    private Long limit;
    private Long escrow;
}
