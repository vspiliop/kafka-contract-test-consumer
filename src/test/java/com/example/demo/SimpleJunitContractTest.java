package com.example.demo;

import io.github.vspiliop.schema.test.TestCreated;
import io.github.vspiliop.schema.test.TestEvents;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.cloud.contract.stubrunner.junit.StubRunnerExtension;
import org.springframework.cloud.contract.verifier.converter.YamlContract;
import org.springframework.cloud.contract.verifier.messaging.MessageVerifierSender;
import org.springframework.kafka.support.KafkaHeaders;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

import static com.example.demo.TestConfig.getSpecificRecordFromJsonWithUnionTypes;
import static org.assertj.core.api.BDDAssertions.then;

@Slf4j
public class SimpleJunitContractTest {

    KafkaContractTestConsumerApplication application = new KafkaContractTestConsumerApplication();

    @RegisterExtension
    StubRunnerExtension contractProducer = new StubRunnerExtension()
            .downloadStub("com.github.vspiliop", "kafka-contract-test-producer", "0.0.1-SNAPSHOT")
            .withConsumerName("service-kafka-consumer-1")
            .withStubPerConsumer(true)
            .messageVerifierSender(new MessageVerifierSender<>() {

                @Override
                public void send(Object message, String destination, @Nullable YamlContract contract) {}

                @Override
                public <T> void send(T payload, Map<String, Object> headers, String destination, @Nullable YamlContract contract) {
                    Map<String, Object> newHeaders = headers != null ? new HashMap<>(headers) : new HashMap<>();
                    newHeaders.put(KafkaHeaders.TOPIC, destination);
                    log.info("Got message [{}]", payload);
                    application.listen(getSpecificRecordFromJsonWithUnionTypes((String) payload, TestEvents.class));
                }
            });

    @Test
    public void when_testCreatedEvent_is_received_do_sth_and_validate() {
        contractProducer.trigger("testCreatedEvent");

        // then
        Awaitility.await().untilAsserted(() -> {
            then(application.event).isNotNull();
            then(application.event.getType()).contains("TestCreated");
            then(application.event.getCorrelationId()).contains("5d1f9fef-e0dc-4f3d-a7e4-72d2220dd827");
            then(((TestCreated) application.event.getPayload()).getText()).contains("this is a test created event");
            then(((TestCreated) application.event.getPayload()).getTestNumber()).contains("first test");
        });
    }

}
