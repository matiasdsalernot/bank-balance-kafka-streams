package com.tauliatrade.securities.stepdefs;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tauliatrade.domain.Investor;
import com.tauliatrade.domain.Security;
import com.tauliatrade.services.InvestorServiceApp;
import com.tauliatrade.services.SecurityServiceApp;
import io.cucumber.java.After;
import io.cucumber.java.Before;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SecurityCreationStepDefs {

    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private KafkaContainer kafkaContainer;
    private KafkaProducer<String, String> kafkaProducer;
    private String investorId;
    private KafkaConsumer<String, String> consumer;
    private InvestorServiceApp investorServiceApp;
    private SecurityServiceApp securityServiceApp;

    @Before
    public void setUp() throws ExecutionException, InterruptedException {
        kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.1.0"));
        kafkaContainer.start();
        kafkaProducer = new KafkaProducer<>(
                Map.of(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers(),
                        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
                )
        );
        securityServiceApp = new SecurityServiceApp(kafkaContainer.getBootstrapServers());
        investorServiceApp = new InvestorServiceApp(kafkaContainer.getBootstrapServers());
        Properties properties = new Properties();
        properties.put("bootstrap.servers", kafkaContainer.getBootstrapServers());
        properties.put("connections.max.idle.ms", 10000);
        properties.put("request.timeout.ms", 5000);
        try (AdminClient client = AdminClient.create(properties)) {
            CreateTopicsResult result = client.createTopics(toTopics(List.of(
                    InvestorServiceApp.INVESTORS_TOPIC,
                    InvestorServiceApp.SECURITIES_TOPIC,
                    InvestorServiceApp.REJECTED_SECURITY_CREATION_REQUESTS_TOPIC,
                    InvestorServiceApp.SECURITY_CREATION_REQUEST_TOPIC,
                    InvestorServiceApp.ESCROW_INVESTOR_WITH_SECURITY_TOPIC,
                    InvestorServiceApp.INVESTOR_LIMITS_TOPIC
            )));
            result.all().get();
        }
    }

    private Collection<NewTopic> toTopics(List<String> investorsTopic) {
        return investorsTopic.stream()
                .map(topicName -> new NewTopic(topicName, 1, (short)1))
                .collect(Collectors.toList());
    }


    @After
    public void tearDown() {
        securityServiceApp.stop();
        investorServiceApp.stop();
        kafkaContainer.stop();
    }

    @Given("the investor {string} with id {int} and a limit of {int}")
    public void theInvestorWithIdAndALimitOf(String arg0, int arg1, int arg2) throws JsonProcessingException, ExecutionException, InterruptedException {
        investorId = String.valueOf(arg1);
        kafkaProducer.send(new ProducerRecord<>(InvestorServiceApp.INVESTORS_TOPIC, investorId, OBJECT_MAPPER.writeValueAsString(Investor.builder().id(String.valueOf(arg1)).name(arg0).limit(Integer.valueOf(arg2).longValue()).build()))).get();
    }

    @When("I send the security creation request with id {int} and amount of {int} to the topic {string}")
    public void iSendTheSecurityCreationRequestWithIdAndAmountOfToTheTopic(int arg0, int arg1, String arg2) throws ExecutionException, InterruptedException, JsonProcessingException {
        final String securityId = String.valueOf(arg0);
        final String security = OBJECT_MAPPER.writeValueAsString(Security.builder().id(securityId).investorId(investorId).amount(Integer.valueOf(arg1).longValue()).build());
        kafkaProducer.send(new ProducerRecord<>(InvestorServiceApp.SECURITY_CREATION_REQUEST_TOPIC, securityId, security)).get();

        investorServiceApp.run();
    }

    @Then("a security is created in the {string} topic")
    public void aSecurityIsCreatedInTheTopic(String arg0) {
        consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "test" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
        ));
        consumer.subscribe(Collections.singletonList(arg0));
        final ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(15));

        assertEquals(1, records.count());
    }

    @And("the investor's new limit of {int} is published in the topic {string}")
    public void theInvestorSNewLimitOfIsPublishedInTheTopic(int arg0, String arg1) {
        consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "test" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
        ));
        consumer.subscribe(Collections.singletonList(arg1));
        final ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(15));
        final long expectedNewLimit = Integer.valueOf(arg0).longValue();
        assertEquals(1, records.count());

        records.forEach(record -> assertLimit(expectedNewLimit, record));
    }

    private void assertLimit(long expectedNewLimit, org.apache.kafka.clients.consumer.ConsumerRecord<String, String> record) {
        try {
            assertEquals(expectedNewLimit, OBJECT_MAPPER.readValue(record.value(), Investor.class).getLimit());
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Then("the request is rejected and published in the topic {string}")
    public void theRequestIsRejectedAndPublishedInTheTopic(String arg0) {
    }

}
