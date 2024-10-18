package com.github.mat_sik.kafka_consumer.consumer.offset;

import com.github.mat_sik.kafka_consumer.consumer.ToCommitQueueHandler;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.Semaphore;
import java.util.logging.Logger;

public class ToCommitOffsetsHandler {

    private static final Logger LOGGER = Logger.getLogger(ToCommitOffsetsHandler.class.getName());

    private final Map<TopicPartition, Long> toCommitOffsets;
    private final ToCommitQueueHandler toCommitQueueHandler;
    private final UncommitedOffsetsHandler uncommitedOffsetsHandler;

    private final Semaphore mutex;

    public ToCommitOffsetsHandler(
            ToCommitQueueHandler toCommitQueueHandler,
            UncommitedOffsetsHandler uncommitedOffsetsHandler
    ) {
        this.toCommitOffsets = new HashMap<>();
        this.toCommitQueueHandler = toCommitQueueHandler;
        this.uncommitedOffsetsHandler = uncommitedOffsetsHandler;

        this.mutex = new Semaphore(1);
    }

    public void updateCommittedOffsets(Map<TopicPartition, OffsetAndMetadata> committed) {
        toCommitOffsets.clear();
        populateToCommitOffsets(toCommitOffsets, committed);
    }

    private static void populateToCommitOffsets(
            Map<TopicPartition, Long> toCommitOffsets,
            Map<TopicPartition, OffsetAndMetadata> committed
    ) {
        committed.forEach((topicPartition, offsetAndMetadata) -> {
            long offset = offsetAndMetadata == null ? 0 : offsetAndMetadata.offset();
            toCommitOffsets.put(topicPartition, offset);
        });
    }

    public void tryCommitOffsets(Map<TopicPartition, Long> offsets) {
        try {
            mutex.acquire();
            try {
                Optional<Map<TopicPartition, OffsetAndMetadata>> toCommitOffsets = getToCommitOffsets(offsets);
                toCommitOffsets.ifPresent(toCommitQueueHandler::add);
            } finally {
                mutex.release();
            }
        } catch (InterruptedException ex) {
            LOGGER.severe(ex.getMessage());
        }
    }

    private Optional<Map<TopicPartition, OffsetAndMetadata>> getToCommitOffsets(Map<TopicPartition, Long> offsets) {
        Map<TopicPartition, OffsetAndMetadata> toCommitOffsets = new HashMap<>();

        offsets.forEach(((topicPartition, offset) -> {
            Optional<OffsetAndMetadata> toCommitOffset = getToCommitOffset(topicPartition, offset);
            toCommitOffset.ifPresent(offsetValue -> toCommitOffsets.put(topicPartition, offsetValue));
        }));

        if (toCommitOffsets.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(toCommitOffsets);
    }

    private Optional<OffsetAndMetadata> getToCommitOffset(TopicPartition topicPartition, long offset) {
        Long toCommitOffset = toCommitOffsets.get(topicPartition);
        if (toCommitOffset == null) {
            LOGGER.severe("Unregistered TopicPartition");
            return Optional.empty();
        }

        if (toCommitOffset != offset) {
            return Optional.empty();
        }

        OptionalLong greatestOffset = uncommitedOffsetsHandler.getGreatestReadyToCommitOffset(topicPartition, offset);
        if (greatestOffset.isEmpty()) {
            return Optional.empty();
        }
        long greatestOffsetValue = greatestOffset.getAsLong();

        toCommitOffsets.put(topicPartition, greatestOffsetValue);

        return Optional.of(new OffsetAndMetadata(greatestOffsetValue));
    }
}
