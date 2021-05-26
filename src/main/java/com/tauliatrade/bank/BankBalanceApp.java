package com.tauliatrade.bank;

import com.tauliatrade.bank.serde.JsonSerde;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

import java.util.Map;
import java.util.Properties;

public class BankBalanceApp {

    public static void main(String[] args) {
        final BankBalanceApp bankBalanceApp = new BankBalanceApp();
        bankBalanceApp.run();
    }

    private void run() {
        final Properties streamsProperties = createStreamsProperties();
        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        final JsonSerde<Investor> investorJsonSerde = new JsonSerde<>(Investor.class);
        final KTable<String, Investor> investors = streamsBuilder.table("investors", Consumed.with(Serdes.String(), investorJsonSerde));

        final JsonSerde<SecurityCreationRequest> valueSerde = new JsonSerde<>(SecurityCreationRequest.class);
        streamsBuilder.stream("security-creation-request", Consumed.with(Serdes.String(), valueSerde))
                .selectKey((key, value) -> value.getInvestorId())
                .join(investors, JoinedInvestorSecurityCreationRequest::new)
                .groupByKey()
                .aggregate(Investor::new, (key, value, aggregate) -> calculateNewInvestorLimit(value, aggregate))
                .toStream()
                .to("investor-limits", Produced.with(Serdes.String(), investorJsonSerde));

        final Map<String, KStream<String, JoinedInvestorSecurityCreationRequest>> branchedStreams = joinedInvestorSecurityCreationRequestKStream
                .split()
                .branch((key, value) -> belowInvestorLimit(value), Branched.as("belowInvestorLimit"))
                .branch((key, value) -> aboveInvestorLimit(value), Branched.as("aboveInvestorLimit"))
                .noDefaultBranch();

        branchedStreams.get("aboveInvestorLimit")
                .map((key, value) -> new KeyValue<>(value.securityCreationRequest.getId(), value.securityCreationRequest))
                .to("rejected-security-creation-requests", Produced.with(Serdes.String(), ));

        final KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), streamsProperties);

        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }

    private Investor calculateNewInvestorLimit(JoinedInvestorSecurityCreationRequest value, Investor aggregate) {
        aggregate.setId(value.investor.getId());
        aggregate.setName(value.investor.getName());
        aggregate.calculateLimit(value.investor.getLimit(), value.securityCreationRequest.getAmount());
        return aggregate;
    }

    private boolean aboveInvestorLimit(JoinedInvestorSecurityCreationRequest value) {
        return !belowInvestorLimit(value);
    }

    private boolean belowInvestorLimit(JoinedInvestorSecurityCreationRequest value) {
        return value.investor.getLimit() - value.securityCreationRequest.getAmount() >= 0;
    }

    private Properties createStreamsProperties() {
        return new Properties();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private static class JoinedInvestorSecurityCreationRequest {
        private SecurityCreationRequest securityCreationRequest;
        private Investor investor;

    }
}
