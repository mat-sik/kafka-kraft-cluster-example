package com.github.mat_sik.kafka_consumer;

import com.github.mat_sik.kafka_consumer.consumer.ContinuousConsumer;
import com.github.mat_sik.kafka_consumer.consumer.ContinuousConsumerRebalanceListener;
import com.github.mat_sik.kafka_consumer.consumer.ToCommitQueueHandler;
import com.github.mat_sik.kafka_consumer.consumer.controller.ListenerProcessingController;
import com.github.mat_sik.kafka_consumer.consumer.controller.ProcessorsProcessingController;
import com.github.mat_sik.kafka_consumer.consumer.offset.OffsetHandler;
import com.github.mat_sik.kafka_consumer.consumer.offset.ToCommitOffsetsHandler;
import com.github.mat_sik.kafka_consumer.consumer.offset.UncommitedOffsetsHandler;
import com.mongodb.client.MongoCollection;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.bson.Document;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

@SpringBootApplication
@ConfigurationPropertiesScan
public class KafkaConsumerApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaConsumerApplication.class, args);
    }

    @Bean
    public CommandLineRunner commandLineRunner(
            Admin admin,
            Properties kafkaConsumerProperties,
            MongoCollection<Document> collection
    ) {
        return _ -> {
            List<String> topicNames = List.of("my-topic");
            ensureTopicsExists(admin, topicNames);

            BlockingQueue<ConsumerRecords<String, String>> toProcessQueue = new LinkedBlockingQueue<>();
            ConcurrentLinkedQueue<Map<TopicPartition, OffsetAndMetadata>> toCommitQueue = new ConcurrentLinkedQueue<>();
            var toCommitQueueHandler = new ToCommitQueueHandler(toCommitQueue);

            ReadWriteLock readWriteLock = new ReentrantReadWriteLock(true);

            Consumer<String, String> consumer = new KafkaConsumer<String, String>(kafkaConsumerProperties);

            var uncommitedOffsetsHandler = new UncommitedOffsetsHandler();
            var toCommitOffsetsHandler = new ToCommitOffsetsHandler(toCommitQueueHandler, uncommitedOffsetsHandler);

            var offsetHandler = new OffsetHandler(uncommitedOffsetsHandler, toCommitOffsetsHandler);

            var atomicBoolean = new AtomicBoolean(true);

            var processorsProcessingController = new ProcessorsProcessingController(readWriteLock.readLock(), atomicBoolean);
            var listenerProcessingController = new ListenerProcessingController(readWriteLock.writeLock(), atomicBoolean);

            ContinuousConsumerRebalanceListener continuousConsumerRebalanceListener = new ContinuousConsumerRebalanceListener(
                    consumer,
                    listenerProcessingController,
                    toCommitQueueHandler,
                    toCommitOffsetsHandler,
                    uncommitedOffsetsHandler
            );

            var continuousConsumer = new ContinuousConsumer(
                    consumer,
                    topicNames,
                    continuousConsumerRebalanceListener, toProcessQueue,
                    toCommitQueueHandler,
                    offsetHandler,
                    processorsProcessingController,
                    collection
            );

            continuousConsumer.run();
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
