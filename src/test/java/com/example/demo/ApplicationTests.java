package com.example.demo;

import io.github.vspiliop.schema.test.TestCreated;
import io.github.vspiliop.schema.test.TestEvents;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.contract.stubrunner.StubTrigger;
import org.springframework.cloud.contract.stubrunner.spring.AutoConfigureStubRunner;
import org.springframework.cloud.contract.stubrunner.spring.StubRunnerProperties;
import org.springframework.cloud.contract.verifier.converter.YamlContract;
import org.springframework.cloud.contract.verifier.messaging.MessageVerifierSender;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.BDDAssertions.then;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
		classes = {TestConfig.class, KafkaContractTestConsumerApplication.class})
@AutoConfigureStubRunner(
		ids = "com.example:kafka-contract-test-producer:0.0.1-SNAPSHOT",
		stubsMode = StubRunnerProperties.StubsMode.LOCAL,
		stubsPerConsumer = true)
@Testcontainers
@ActiveProfiles("test")
@Slf4j
public class ApplicationTests {

	@Container
	static KafkaContainer kafka
			= new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.3.1"));

	@DynamicPropertySource
	static void kafkaProperties(DynamicPropertyRegistry registry) {
		registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
	}

	@Autowired
	StubTrigger producer;

	@Autowired
	KafkaContractTestConsumerApplication application;

	@Test
	public void when_testCreatedEvent_is_received_do_sth_and_validate() {
		// when
		producer.trigger("testCreatedEvent");

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

@Configuration
@Slf4j
class TestConfig {

	@Bean
	MessageVerifierSender<Message<?>> standaloneMessageVerifier(KafkaTemplate kafkaTemplate) {
		return new MessageVerifierSender<>() {

			@Override
			public void send(Message<?> message, String destination, @Nullable YamlContract contract) {
			}

			@Override
			public <T> void send(T payload, Map<String, Object> headers, String destination, @Nullable YamlContract contract) {
				Map<String, Object> newHeaders = headers != null ? new HashMap<>(headers) : new HashMap<>();
				newHeaders.put(KafkaHeaders.TOPIC, destination);
				log.info("Got message [{}]", payload);
				kafkaTemplate.send(MessageBuilder.createMessage(
						getSpecificRecordFromJsonWithUnionTypes((String) payload, TestEvents.class),
						new MessageHeaders(newHeaders))
				);
			}
		};
	}

	static <T extends SpecificRecord> T getSpecificRecordFromJsonWithUnionTypes(String JsonWithUnionTypes,
																				Class<T> specificRecord) {
		DatumReader<T> reader = new SpecificDatumReader<>(specificRecord);
		try {
			Decoder decoder = DecoderFactory.get().jsonDecoder(
					(Schema) specificRecord.getField("SCHEMA$").get(null), JsonWithUnionTypes);
			return reader.read(null, decoder);
		} catch (IOException | NoSuchFieldException | IllegalAccessException e) {
			return null;
		}
	}
}