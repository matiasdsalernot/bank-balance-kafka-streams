package com.tauliatrade.bank;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SecurityCreationRequest {

    private String id;
    private List<String> fundingRequestId;
    private Long amount;
    private String investorId;
}
