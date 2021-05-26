package com.tauliatrade.bank;

import com.tauliatrade.bank.serde.JsonSerde;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class BankBalanceApp {

    public static final String SECURITY_CREATION_REQUEST_TOPIC = "security-creation-request-topic";
    public static final String INVESTORS_TOPIC = "investors-topic";
    public static final String INVESTORS_LOG_TOPIC = "investors-log-topic";
    public static final String INVESTOR_LIMITS_TOPIC = "investor-limits-topic";
    private final String bootstrapServers;

    public BankBalanceApp(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public static void main(String[] args) {
        final BankBalanceApp bankBalanceApp = new BankBalanceApp("localhost:9092");
        bankBalanceApp.run();
    }

    private void run() {
        final Properties streamsProperties = createStreamsProperties();
        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        final JsonSerde<Investor> investorJsonSerde = new JsonSerde<>(Investor.class);
        final KTable<String, Investor> investors = streamsBuilder.table(INVESTORS_TOPIC, Consumed.with(Serdes.String(), investorJsonSerde));
        investors.toStream().to(INVESTORS_LOG_TOPIC, Produced.with(Serdes.String(), investorJsonSerde));

        final JsonSerde<SecurityCreationRequest> valueSerde = new JsonSerde<>(SecurityCreationRequest.class);
        streamsBuilder.stream(SECURITY_CREATION_REQUEST_TOPIC, Consumed.with(Serdes.String(), valueSerde))
                .selectKey((key, value) -> value.getInvestorId())
                .join(investors, JoinedInvestorSecurityCreationRequest::new)
                .groupByKey()
                .aggregate(Investor::new, (key, value, aggregate) -> calculateNewInvestorLimit(value, aggregate), Materialized.with(Serdes.String(), investorJsonSerde))
                .toStream()
                .to(INVESTOR_LIMITS_TOPIC, Produced.with(Serdes.String(), investorJsonSerde));

        final KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), streamsProperties);

        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }

    private Investor calculateNewInvestorLimit(JoinedInvestorSecurityCreationRequest value, Investor aggregate) {
        aggregate.setId(value.investor.getId());
        aggregate.setName(value.investor.getName());
        aggregate.calculateLimit(value);
        return aggregate;
    }

    private Properties createStreamsProperties() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "investor-limits-streams");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        return properties;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class JoinedInvestorSecurityCreationRequest {
        private SecurityCreationRequest securityCreationRequest;
        private Investor investor;

    }
}
