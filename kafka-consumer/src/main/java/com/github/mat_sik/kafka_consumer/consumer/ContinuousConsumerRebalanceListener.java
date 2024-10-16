package com.github.mat_sik.kafka_consumer.consumer;

import com.github.mat_sik.kafka_consumer.consumer.offset.ToCommitOffsetsHandler;
import com.github.mat_sik.kafka_consumer.consumer.offset.UncommitedOffsetsHandler;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.logging.Logger;

public class ContinuousConsumerRebalanceListener implements ConsumerRebalanceListener {

    private static final Logger LOGGER = Logger.getLogger(ContinuousConsumerRebalanceListener.class.getName());

    private final Consumer<String, String> consumer;
    private final Lock writeLock;
    private final ToCommitQueueHandler toCommitQueueHandler;
    private final ToCommitOffsetsHandler toCommitOffsetsHandler;
    private final UncommitedOffsetsHandler uncommitedOffsetsHandler;

    public ContinuousConsumerRebalanceListener(
            Consumer<String, String> consumer,
            Lock writeLock,
            ToCommitQueueHandler toCommitQueueHandler,
            ToCommitOffsetsHandler toCommitOffsetsHandler,
            UncommitedOffsetsHandler uncommitedOffsetsHandler
    ) {
        this.consumer = consumer;
        this.writeLock = writeLock;
        this.toCommitQueueHandler = toCommitQueueHandler;
        this.toCommitOffsetsHandler = toCommitOffsetsHandler;
        this.uncommitedOffsetsHandler = uncommitedOffsetsHandler;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
        try {
            LOGGER.info("### onPartitionsRevoked CALLED ###");
            writeLock.lockInterruptibly();
            LOGGER.info("### onPartitionsRevoked GOT LOCK ###");
            try {
                Optional<Map<TopicPartition, OffsetAndMetadata>> toCommitOffsets = toCommitQueueHandler.poolReadyToCommitOffsets();
                toCommitOffsets.ifPresent(consumer::commitSync);
            } finally {
                writeLock.unlock();
                LOGGER.info("### onPartitionsRevoked DONE ###");
            }
        } catch (InterruptedException ex) {
            LOGGER.severe("Got interrupted, exception message: " + ex.getMessage());
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
        LOGGER.info("### onPartitionsAssigned CALLED ###");

        Set<TopicPartition> topicPartitions = new HashSet<>(collection);
        Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(topicPartitions);

        uncommitedOffsetsHandler.updateUncommittedOffsets(topicPartitions);

        toCommitOffsetsHandler.updateCommittedOffsets(committed);
        LOGGER.info("### onPartitionsAssigned DONE ###");
    }

}
