package com.tauliatrade.services;

import com.tauliatrade.domain.Investor;
import com.tauliatrade.domain.Security;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

public class InvestorServiceApp {

    public static void main(String[] args) {
        final InvestorServiceApp investorServiceApp = new InvestorServiceApp();
        investorServiceApp.run();
    }

    private void run() {
        final Properties streamsProperties = createStreamsProperties();
        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        final KTable<String, Investor> investors = streamsBuilder.table("investors", Consumed.with(Serdes.String(), new JsonSerde<>(Investor.class)));
        final KStream<String, Security> securities = streamsBuilder.stream("securities", Consumed.with(Serdes.String(), new JsonSerde<>(Security.class)));


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
