package com.tauliatrade.bank;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

public class BankBalanceApp {

    public static void main(String[] args) {
        final BankBalanceApp bankBalanceApp = new BankBalanceApp();
        bankBalanceApp.run();
    }

    private void run() {
        final Properties streamsProperties = createStreamsProperties();
        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        streamsBuilder.stream("security-creation-request")
                /* Stream processing here */
                .to("investor-limits");

        final KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), streamsProperties);

        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }

    private Properties createStreamsProperties() {
        return new Properties();
    }
}
