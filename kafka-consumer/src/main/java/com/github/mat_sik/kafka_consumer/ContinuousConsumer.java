package com.github.mat_sik.kafka_consumer;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Logger;

public class ContinuousConsumer implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(ContinuousConsumer.class.getName());

    private static final Duration POLL_DURATION = Duration.ofMillis(100);

    private final KafkaConsumer<String, String> consumer;
    private final Collection<String> topicNames;

    private final BlockingQueue<ConsumerRecords<String, String>> toProcessQueue;
    private final ConcurrentLinkedQueue<Map<TopicPartition, OffsetAndMetadata>> toCommitQueue;

    public ContinuousConsumer(
            Properties kafkaConsumerProperties,
            Collection<String> topicNames,
            BlockingQueue<ConsumerRecords<String, String>> toProcessQueue,
            ConcurrentLinkedQueue<Map<TopicPartition, OffsetAndMetadata>> toCommitQueue
    ) {
        this.consumer = new KafkaConsumer<>(kafkaConsumerProperties);
        this.topicNames = topicNames;
        this.toProcessQueue = toProcessQueue;
        this.toCommitQueue = toCommitQueue;
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
        try {
            boolean processorsRunning = false;
            for (; ; ) {
                // After wakeup() was called on consumer, poll() will throw WakeupException.
                ConsumerRecords<String, String> records = consumer.poll(POLL_DURATION);
                if (!processorsRunning) {
                    processorsRunning = setUpProcessors();
                }

                if (!records.isEmpty()) {
                    toProcessQueue.put(records);
                }

                Map<TopicPartition, OffsetAndMetadata> toCommitMap = toCommitQueue.poll();
                if (toCommitMap != null) {
                    logToCommitMap(toCommitMap);
                    consumer.commitSync(toCommitMap);
                }
            }
        } catch (InterruptedException ex) {
            LOGGER.severe(ex.getMessage());
            shutdown();
        }
    }

    private boolean setUpProcessors() {
        Set<TopicPartition> topicPartitions = consumer.assignment();
        if (topicPartitions.isEmpty()) {
            return false;
        }
        Map<TopicPartition, OffsetAndMetadata> commited = consumer.committed(topicPartitions);

        var offsetCommiter = new OffsetCommitHandler(commited, toCommitQueue);

        Thread.ofVirtual().name("processor-1").start(new RecordBatchProcessor(toProcessQueue, offsetCommiter));
        Thread.ofVirtual().name("processor-2").start(new RecordBatchProcessor(toProcessQueue, offsetCommiter));
        Thread.ofVirtual().name("processor-3").start(new RecordBatchProcessor(toProcessQueue, offsetCommiter));

        return true;
    }

    private void logToCommitMap(Map<TopicPartition, OffsetAndMetadata> toCommitMap) {
        StringBuilder builder = new StringBuilder("## TO COMMIT MAP:");
        toCommitMap.forEach(((topicPartition, offsetAndMetadata) -> {
            builder.append(" | PARTITION: ")
                    .append(topicPartition.partition())
                    .append(" OFFSET: ")
                    .append(offsetAndMetadata.offset());
        }));

        builder.append(" | ##");

        LOGGER.info(builder.toString());
    }

    public void shutdown() {
        consumer.wakeup();
    }

}
