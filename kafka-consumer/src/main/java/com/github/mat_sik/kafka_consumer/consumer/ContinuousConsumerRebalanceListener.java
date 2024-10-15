package com.github.mat_sik.kafka_consumer.consumer;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.logging.Logger;

public class ContinuousConsumerRebalanceListener implements ConsumerRebalanceListener {

    private static final Logger LOGGER = Logger.getLogger(ContinuousConsumerRebalanceListener.class.getName());

    private final KafkaConsumer<String, String> consumer;
    private final Lock writeLock;
    private final ToCommitQueueHandler toCommitQueueHandler;

    public ContinuousConsumerRebalanceListener(
            KafkaConsumer<String, String> consumer,
            Lock writeLock,
            ToCommitQueueHandler toCommitQueueHandler
    ) {
        this.consumer = consumer;
        this.writeLock = writeLock;
        this.toCommitQueueHandler = toCommitQueueHandler;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
        try {
            writeLock.lockInterruptibly();
            try {
                Optional<Map<TopicPartition, OffsetAndMetadata>> toCommitOffsets = toCommitQueueHandler.poolReadyToCommitOffsets();
                toCommitOffsets.ifPresent(consumer::commitSync);
            } finally {
                writeLock.unlock();
            }
        } catch (InterruptedException ex) {
            LOGGER.severe("Got interrupted, exception message: " + ex.getMessage());
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
        // update OffsetHandler with information about new TopicPartitions
    }

}
