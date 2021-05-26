package com.tauliatrade.bank;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class ProducerApp {

    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) {
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(
                Map.of(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
                )
        );

//        sendInitialData(kafkaProducer);

        sendSecondData(kafkaProducer);
    }

    private static void sendSecondData(KafkaProducer<String, String> kafkaProducer) {
//        List.of(
//                Investor.builder().id("1").name("VVBU").limit(31_000L).state(Investor.InvestorState.UPDATE).build()
//        ).forEach(investor -> sendInvestorToKafka(kafkaProducer, investor));

        List.of(
                SecurityCreationRequest.builder().id("12").fundingRequestId(Collections.emptyList()).investorId("1").amount(500L).build()
        ).forEach(securityCreationRequest -> sendSecRequestToKafka(kafkaProducer, securityCreationRequest));
    }

    private static void sendInitialData(KafkaProducer<String, String> kafkaProducer) {
        List.of(
                Investor.builder().id("1").name("VVBU").limit(10_000L).build(),
                Investor.builder().id("2").name("CMPJ").limit(20_000L).build(),
                Investor.builder().id("3").name("River Bank").limit(15_000L).build(),
                Investor.builder().id("4").name("West Bank").limit(5_000L).build()
        ).forEach(investor -> sendInvestorToKafka(kafkaProducer, investor));

        List.of(
                SecurityCreationRequest.builder().id("1").fundingRequestId(Collections.emptyList()).investorId("1").amount(500L).build(),
                SecurityCreationRequest.builder().id("2").fundingRequestId(Collections.emptyList()).investorId("1").amount(400L).build(),
                SecurityCreationRequest.builder().id("3").fundingRequestId(Collections.emptyList()).investorId("1").amount(600L).build(),
                SecurityCreationRequest.builder().id("4").fundingRequestId(Collections.emptyList()).investorId("2").amount(200L).build(),
                SecurityCreationRequest.builder().id("5").fundingRequestId(Collections.emptyList()).investorId("2").amount(600L).build(),
                SecurityCreationRequest.builder().id("6").fundingRequestId(Collections.emptyList()).investorId("2").amount(800L).build(),
                SecurityCreationRequest.builder().id("7").fundingRequestId(Collections.emptyList()).investorId("2").amount(100L).build(),
                SecurityCreationRequest.builder().id("8").fundingRequestId(Collections.emptyList()).investorId("3").amount(200L).build(),
                SecurityCreationRequest.builder().id("9").fundingRequestId(Collections.emptyList()).investorId("3").amount(300L).build(),
                SecurityCreationRequest.builder().id("10").fundingRequestId(Collections.emptyList()).investorId("4").amount(500L).build(),
                SecurityCreationRequest.builder().id("11").fundingRequestId(Collections.emptyList()).investorId("4").amount(700L).build()
        ).forEach(securityCreationRequest -> sendSecRequestToKafka(kafkaProducer, securityCreationRequest));
    }

    private static void sendInvestorToKafka(KafkaProducer<String, String> kafkaProducer, Investor investor) {
        try {
            kafkaProducer.send(new ProducerRecord<>("investors-topic", investor.getId(), OBJECT_MAPPER.writeValueAsString(investor))).get();
        } catch (InterruptedException | ExecutionException | JsonProcessingException e) {
            throw new RuntimeException("Error sending investor", e);
        }
    }

    private static void sendSecRequestToKafka(KafkaProducer<String, String> kafkaProducer, SecurityCreationRequest securityCreationRequest) {
        try {
            kafkaProducer.send(new ProducerRecord<>("security-creation-request-topic", securityCreationRequest.getId(), OBJECT_MAPPER.writeValueAsString(securityCreationRequest))).get();
        } catch (InterruptedException | ExecutionException | JsonProcessingException e) {
            throw new RuntimeException("Error sending investor", e);
        }
    }
}
