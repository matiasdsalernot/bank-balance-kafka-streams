package com.tauliatrade.services;

import com.tauliatrade.domain.Investor;
import com.tauliatrade.domain.JoinInvestorSecurity;
import com.tauliatrade.domain.Security;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;
@Slf4j
public class InvestorServiceApp {

    public static final String INVESTORS_TOPIC = "investors";
    public static final String SECURITIES_TOPIC = "securities";
    public static final String REJECTED_SECURITY_CREATION_REQUESTS_TOPIC = "rejected-security-creation-requests";
    public static final String SECURITY_CREATION_REQUEST_TOPIC = "security-creation-request";
    public static final String ESCROW_INVESTOR_WITH_SECURITY_TOPIC = "escrow-investor-with-security";
    public static final String INVESTOR_LIMITS_TOPIC = "investor-limits";
    private final String bootstrapServer;
    private KafkaStreams kafkaStreams;

    public InvestorServiceApp(String bootstrapServer) {
        this.bootstrapServer = bootstrapServer;
    }

    public static void main(String[] args) {
        final InvestorServiceApp investorServiceApp = new InvestorServiceApp("localhost:9092");
        investorServiceApp.run();
    }

    public void run() {
        final Properties streamsProperties = createStreamsProperties();
        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        final JsonSerde<Investor> investorJsonSerde = new JsonSerde<>(Investor.class);
        final KTable<String, Investor> investors = streamsBuilder.table(INVESTORS_TOPIC, Consumed.with(Serdes.String(), investorJsonSerde));
        final JsonSerde<Security> securityJsonSerde = new JsonSerde<>(Security.class);
        final KStream<String, Security> securities = streamsBuilder.stream(SECURITIES_TOPIC, Consumed.with(Serdes.String(), securityJsonSerde));

        final KStream<String, Security> rejectedSecuritiesStream = streamsBuilder.stream(REJECTED_SECURITY_CREATION_REQUESTS_TOPIC, Consumed.with(Serdes.String(), securityJsonSerde));

        final KTable<String, Investor> investorWithUpdatedLimits = securities
                .selectKey((key, value) -> value.getInvestorId())
                .join(investors, JoinInvestorSecurity::new)
                .groupByKey()
                .aggregate(Investor::new, (key, value, aggregate) -> calculateNewInvestorLimit(value, aggregate), Materialized.with(Serdes.String(), investorJsonSerde));

        final KTable<String, Investor> fullyUpdatedInvestor = rejectedSecuritiesStream
                .selectKey((key, value) -> value.getInvestorId())
                .join(investorWithUpdatedLimits, JoinInvestorSecurity::new)
                .groupByKey()
                .aggregate(Investor::new, (key, value, aggregate) -> unescrowInvestor(value, aggregate), Materialized.with(Serdes.String(), investorJsonSerde));

        final JsonSerde<JoinInvestorSecurity> investorSecurityJsonSerde = new JsonSerde<>(JoinInvestorSecurity.class);

        streamsBuilder.stream(SECURITY_CREATION_REQUEST_TOPIC, Consumed.with(Serdes.String(), securityJsonSerde))
                .selectKey((key, value) -> value.getInvestorId())
                .join(fullyUpdatedInvestor, JoinInvestorSecurity::new)
                .groupByKey()
                .aggregate(JoinInvestorSecurity::new, (key, value, aggregate) -> calculateFutureLimit(value, aggregate), Materialized.with(Serdes.String(), investorSecurityJsonSerde))
                .toStream()
                .to(ESCROW_INVESTOR_WITH_SECURITY_TOPIC, Produced.with(Serdes.String(), investorSecurityJsonSerde));

        fullyUpdatedInvestor
                .toStream()
                .to(INVESTOR_LIMITS_TOPIC, Produced.with(Serdes.String(), investorJsonSerde));

        kafkaStreams = new KafkaStreams(streamsBuilder.build(), streamsProperties);
        kafkaStreams.setUncaughtExceptionHandler(exception -> {
            log.error("Uncaught error in InvestorServiceApp", exception);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });
        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }

    public void stop(){
        kafkaStreams.close();
    }

    private Investor unescrowInvestor(JoinInvestorSecurity value, Investor aggregate) {
        aggregate.setId(value.getInvestor().getId());
        aggregate.setName(value.getInvestor().getName());
        aggregate.unescrow(value);
        return aggregate;
    }

    private JoinInvestorSecurity calculateFutureLimit(JoinInvestorSecurity value, JoinInvestorSecurity aggregate) {
        aggregate.setSecurity(value.getSecurity());
        aggregate.updateInvestor(value);
        return aggregate;
    }

    private Investor calculateNewInvestorLimit(JoinInvestorSecurity value, Investor aggregate) {
        aggregate.setId(value.getInvestor().getId());
        aggregate.setName(value.getInvestor().getName());
        aggregate.calculateLimit(value);
        return aggregate;
    }


    private Properties createStreamsProperties() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "investor-limits-streams");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServer);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        return properties;
    }
}
