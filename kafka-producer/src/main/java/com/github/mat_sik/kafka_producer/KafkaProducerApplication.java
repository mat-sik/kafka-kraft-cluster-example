package com.github.mat_sik.kafka_producer;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Bean;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@SpringBootApplication
@ConfigurationPropertiesScan
public class KafkaProducerApplication {

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
            List<String> topicNames = List.of(topicName);
            ensureTopicsExists(admin, topicNames);

            var continuousProducer = new ContinuousProducer(kafkaProducer, topicName);
            continuousProducer.run();
        };
    }

    private void ensureTopicsExists(Admin admin, Collection<String> names) throws ExecutionException, InterruptedException {
        Set<String> missingNames = getMissingTopicNames(admin, names);
        if (!missingNames.isEmpty()) {
            createTopics(admin, names);
        }
    }

    private Set<String> getMissingTopicNames(Admin admin, Collection<String> names) throws ExecutionException, InterruptedException {
        ListTopicsResult listTopics = admin.listTopics();
        Set<String> remoteNames = listTopics.names().get();

        return names.stream()
                .filter(name -> !remoteNames.contains(name))
                .collect(Collectors.toSet());
    }

    private void createTopics(Admin admin, Collection<String> names) throws ExecutionException, InterruptedException {
        int partitions = 3;
        short replicationFactor = 3;

        List<NewTopic> topics = names.stream()
                .map(name -> new NewTopic(name, partitions, replicationFactor))
                .toList();

        CreateTopicsResult future = admin.createTopics(topics);

        future.all().get();
    }

}
