package com.github.mat_sik.kafka_consumer;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Bean;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

@SpringBootApplication
@ConfigurationPropertiesScan
public class KafkaConsumerApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaConsumerApplication.class, args);
    }

    @Bean
    public CommandLineRunner commandLineRunner(Admin admin, Properties kafkaConsumerProperties) {
        return _ -> {
            List<String> topicNames = List.of("my-topic");
            ensureTopicExists(admin, topicNames);

            BlockingQueue<ConsumerRecords<String, String>> toProcessQueue = new LinkedBlockingQueue<>();
            ConcurrentLinkedQueue<Map<TopicPartition, OffsetAndMetadata>> toCommitQueue = new ConcurrentLinkedQueue<>();

            var continuousConsumer = new ContinuousConsumer(kafkaConsumerProperties, topicNames, toProcessQueue, toCommitQueue);
            continuousConsumer.run();
        };
    }

    private void ensureTopicExists(Admin admin, Collection<String> names) throws ExecutionException, InterruptedException {
        if (!topicExist(admin, names)) {
            createTopic(admin, names);
        }
    }

    private void createTopic(Admin admin, Collection<String> names) throws ExecutionException, InterruptedException {
        int partitions = 3;
        short replicationFactor = 3;

        List<NewTopic> topics = names.stream()
                .map(name -> new NewTopic(name, partitions, replicationFactor))
                .toList();

        CreateTopicsResult future = admin.createTopics(topics);

        future.all().get();
    }

    private boolean topicExist(Admin admin, Collection<String> names) throws ExecutionException, InterruptedException {
        ListTopicsResult listTopics = admin.listTopics();
        Set<String> remoteNames = listTopics.names().get();
        return remoteNames.containsAll(names);
    }

}
