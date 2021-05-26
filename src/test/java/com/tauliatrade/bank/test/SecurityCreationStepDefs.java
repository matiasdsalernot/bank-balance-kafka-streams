package com.tauliatrade.bank.test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tauliatrade.bank.BankBalanceApp;
import com.tauliatrade.bank.Investor;
import io.cucumber.java.Before;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class SecurityCreationStepDefs {

    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private KafkaContainer kafkaContainer;
    private KafkaProducer<String, String> kafkaProducer;
    private String investorId;
    private KafkaConsumer<String, String> consumer;
    private BankBalanceApp bankBalanceApp;

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
        bankBalanceApp = new BankBalanceApp(kafkaContainer.getBootstrapServers());
        Properties properties = new Properties();
        properties.put("bootstrap.servers", kafkaContainer.getBootstrapServers());
        properties.put("connections.max.idle.ms", 10000);
        properties.put("request.timeout.ms", 5000);
        try (AdminClient client = AdminClient.create(properties)) {
            CreateTopicsResult result = client.createTopics(toTopics(List.of(
                    BankBalanceApp.SECURITY_CREATION_REQUEST_TOPIC,
                    BankBalanceApp.INVESTORS_TOPIC,
                    BankBalanceApp.INVESTORS_LOG_TOPIC,
                    BankBalanceApp.INVESTOR_LIMITS_TOPIC
            )));
            result.all().get();
        }
    }

    private Collection<NewTopic> toTopics(List<String> investorsTopic) {
        return investorsTopic.stream()
                .map(topicName -> new NewTopic(topicName, 1, (short)1))
                .collect(Collectors.toList());
    }

    @Given("the investor {string} with id {int} and a limit of {int}")
    public void theInvestorWithIdAndALimitOf(String arg0, int arg1, int arg2) throws JsonProcessingException, ExecutionException, InterruptedException {
        investorId = String.valueOf(arg1);
        kafkaProducer.send(new ProducerRecord<>(BankBalanceApp.INVESTORS_TOPIC, investorId, OBJECT_MAPPER.writeValueAsString(Investor.builder().id(String.valueOf(arg1)).name(arg0).limit(Integer.valueOf(arg2).longValue()).build()))).get();
    }

    @When("I send the security creation request with id {int} and amount of {int} to the topic {string}")
    public void iSendTheSecurityCreationRequestWithIdAndAmountOfToTheTopic(int arg0, int arg1, String arg2) {
    }

    @Then("the investor's new limit of {int} is published in the topic {string}")
    public void theInvestorSNewLimitOfIsPublishedInTheTopic(int arg0, String arg1) {
    }
}
