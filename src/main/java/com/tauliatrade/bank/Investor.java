package com.tauliatrade.bank;

import com.fasterxml.jackson.annotation.JsonIgnore;
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
    @JsonIgnore
    private boolean first = true;

    public void calculateLimit(Long limit, Long amount) {
        if (first) {
            this.limit = limit;
            this.first = false;
        }
        this.limit = this.limit - amount;
    }
}
