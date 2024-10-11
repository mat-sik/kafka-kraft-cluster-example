package com.github.mat_sik.kafka_consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;

public class ContinuousConsumer implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(ContinuousConsumer.class.getName());

    private static final Duration POLL_DURATION = Duration.ofMillis(100);

    private final KafkaConsumer<String, String> consumer;
    private final Collection<String> topicNames;

    public ContinuousConsumer(Properties kafkaConsumerProperties, Collection<String> topicNames) {
        this.consumer = new KafkaConsumer<>(kafkaConsumerProperties);
        this.topicNames = topicNames;
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(topicNames);
            continuousConsume();
        } catch (WakeupException ex) {
            LOGGER.info(ex.getMessage());
        } catch (Exception ex) {
            LOGGER.severe(ex.getMessage());
        } finally {
            consumer.close();
        }
    }

    private void continuousConsume() {
        for (; ; ) {
            // After wakeup() was called on consumer, poll() will throw WakeupException.
            ConsumerRecords<String, String> records = consumer.poll(POLL_DURATION);
            if (!records.isEmpty()) {
                logPartitionData(records);
            }
            consumer.commitSync();
        }
    }

    private void logPartitionData(ConsumerRecords<String, String> records) {
        LOGGER.info("### NEW BATCH ###");

        Set<TopicPartition> partitions = records.partitions();
        partitions.forEach(topicPartition -> {
            int partitionNumber = topicPartition.partition();
            LOGGER.info("## PARTITION: " + partitionNumber + " ##");

            List<ConsumerRecord<String, String>> partitionRecords = records.records(topicPartition);
            partitionRecords.forEach(record -> LOGGER.info("# record: " + record + " #"));
        });

        LOGGER.info("### END BATCH ###");
    }

    public void shutdown() {
        consumer.wakeup();
    }

}
