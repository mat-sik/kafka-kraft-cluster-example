package com.github.mat_sik.kafka_consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
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
        boolean seekPerformed = false;
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
        StringBuilder builder = new StringBuilder("### NEW BATCH");

        Set<TopicPartition> partitions = records.partitions();
        for (TopicPartition partition : partitions) {
            int partitionNumber = partition.partition();

            List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
            long firstOffset = partitionRecords.getFirst().offset();
            long lastOffset = partitionRecords.getLast().offset();

            builder.append(" | PARTITION: ")
                    .append(partitionNumber)
                    .append(" FIRST OFFSET: ")
                    .append(firstOffset)
                    .append(" LAST OFFSET: ")
                    .append(lastOffset);
        }

        builder.append(" | ###");

        LOGGER.info(builder.toString());
    }

    public void shutdown() {
        consumer.wakeup();
    }

}
