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
import org.apache.kafka.streams.kstream.*;

import java.util.Map;
import java.util.Properties;

@Slf4j
public class SecurityServiceApp {

    private final String bootstrapServer;
    private KafkaStreams kafkaStreams;

    public SecurityServiceApp(String bootstrapServer) {
        this.bootstrapServer = bootstrapServer;
    }


    public static void main(String[] args) {
        final SecurityServiceApp SecurityServiceApp = new SecurityServiceApp("localhost:9092");
        SecurityServiceApp.run();
    }

    public void run() {
        try {
            final Properties streamsProperties = createStreamsProperties();
            final StreamsBuilder streamsBuilder = new StreamsBuilder();

            final JsonSerde<Security> securityJsonSerde = new JsonSerde<>(Security.class);
            final JsonSerde<JoinInvestorSecurity> investorSecurityJsonSerde = new JsonSerde<>(JoinInvestorSecurity.class);


            final Map<String, KStream<String, JoinInvestorSecurity>> branches = streamsBuilder.stream(InvestorServiceApp.ESCROW_INVESTOR_WITH_SECURITY_TOPIC, Consumed.with(Serdes.String(), investorSecurityJsonSerde))
                    .split(Named.as("branches-"))
                    .branch((key, value) -> value.getInvestor().getLimit() < 0, Branched.as("rejected"))
                    .branch((key, value) -> value.getInvestor().getLimit() >= 0, Branched.as("approved"))
                    .noDefaultBranch();

            branches.get("branches-rejected")
                    .mapValues((readOnlyKey, value) -> value.getSecurity())
                    .to(InvestorServiceApp.REJECTED_SECURITY_CREATION_REQUESTS_TOPIC, Produced.with(Serdes.String(), securityJsonSerde));

            branches.get("branches-approved")
                    .mapValues((readOnlyKey, value) -> value.getSecurity())
                    .to(InvestorServiceApp.REJECTED_SECURITY_CREATION_REQUESTS_TOPIC, Produced.with(Serdes.String(), securityJsonSerde));
            this.kafkaStreams = new KafkaStreams(streamsBuilder.build(), streamsProperties);
            kafkaStreams.start();

            Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
        } catch (Exception e) {
            log.error("Error in securityServiceApp.", e);
        }
    }

    public void stop(){
        kafkaStreams.close();
    }

    private Properties createStreamsProperties() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "securities-streams");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServer);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        return properties;
    }
}
