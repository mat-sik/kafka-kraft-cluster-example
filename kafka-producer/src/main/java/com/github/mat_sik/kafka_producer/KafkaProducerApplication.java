package com.github.mat_sik.kafka_producer;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Bean;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@SpringBootApplication
@ConfigurationPropertiesScan
public class KafkaProducerApplication {

	private static final Duration SLEEP_DURATION = Duration.ofMillis(100);

	public static void main(String[] args) {
		SpringApplication.run(KafkaProducerApplication.class, args);
	}

	@Bean
	public CommandLineRunner commandLineRunner(
			Admin admin,
			Producer<String, String> kafkaProducer
	) {
		return _ -> {
			String topicName = "my-topic";
			createTopic(admin, topicName);
			continuousProduce(kafkaProducer, topicName);
		};
	}

	private void createTopic(Admin admin, String name) {
		int partitions = 3;
		short replicationFactor = 3;
		var topic = new NewTopic(name, partitions, replicationFactor);

		CreateTopicsResult future = admin.createTopics(List.of(topic));

		future.all();
	}

	private void continuousProduce(
			Producer<String, String> kafkaProducer,
			String topicName
	) throws ExecutionException, InterruptedException {
		for (int i = 0; i < 100_000; i++) {
			String value = String.valueOf(i);
			ProducerRecord<String, String> record = new ProducerRecord<>(topicName, value, value);

			Future<RecordMetadata> future = kafkaProducer.send(record);

			future.get();

			Thread.sleep(SLEEP_DURATION);
		}
	}

}
