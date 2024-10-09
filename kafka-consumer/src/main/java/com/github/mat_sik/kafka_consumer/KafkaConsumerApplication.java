package com.github.mat_sik.kafka_consumer;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Bean;

import java.util.List;
import java.util.Properties;

@SpringBootApplication
@ConfigurationPropertiesScan
public class KafkaConsumerApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaConsumerApplication.class, args);
	}

	@Bean
	public CommandLineRunner commandLineRunner(
			Admin admin,
			Properties kafkaConsumerProperties
	) {
		return _ -> {
			String topicName = "my-topic";
			createTopic(admin, topicName);

			var consumer = new ContinuousConsumer(kafkaConsumerProperties, List.of(topicName));

			consumer.run();
		};
	}

	private void createTopic(Admin admin, String name) {
		int partitions = 3;
		short replicationFactor = 3;
		var topic = new NewTopic(name, partitions, replicationFactor);

		CreateTopicsResult future = admin.createTopics(List.of(topic));

		future.all();
	}

}
