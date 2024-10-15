package com.github.mat_sik.kafka_consumer.consumer.offset;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.logging.Logger;

public class ToCommitOffsetsHandler {

    private static final Logger LOGGER = Logger.getLogger(ToCommitOffsetsHandler.class.getName());

    private final Map<TopicPartition, Long> toCommitOffsets;
    private final ConcurrentLinkedQueue<Map<TopicPartition, OffsetAndMetadata>> toCommitQueue;
    private final UncommitedOffsetsHandler uncommitedOffsetsHandler;

    private final Semaphore mutex;

    public ToCommitOffsetsHandler(
            Map<TopicPartition, OffsetAndMetadata> committed,
            ConcurrentLinkedQueue<Map<TopicPartition, OffsetAndMetadata>> toCommitQueue,
            UncommitedOffsetsHandler uncommitedOffsetsHandler
    ) {
        this.toCommitOffsets = createToCommitOffsetsTracker(committed);
        this.toCommitQueue = toCommitQueue;
        this.uncommitedOffsetsHandler = uncommitedOffsetsHandler;

        this.mutex = new Semaphore(1);
    }

    private static Map<TopicPartition, Long> createToCommitOffsetsTracker(Map<TopicPartition, OffsetAndMetadata> committed) {
        Map<TopicPartition, Long> toCommitOffsets = new HashMap<>();

        committed.forEach((topicPartition, offsetAndMetadata) -> {
            long offset = offsetAndMetadata == null ? 0 : offsetAndMetadata.offset();
            toCommitOffsets.put(topicPartition, offset);
        });

        return toCommitOffsets;
    }

    public void tryCommitOffsets(Map<TopicPartition, Long> offsets) {
        try {
            Map<TopicPartition, OffsetAndMetadata> toCommitMap = new HashMap<>();
            mutex.acquire();
            try {
                offsets.forEach(((topicPartition, offset) -> {
                    Optional<OffsetAndMetadata> toCommitOffset = getToCommitOffset(topicPartition, offset);
                    toCommitOffset.ifPresent(offsetValue -> toCommitMap.put(topicPartition, offsetValue));
                }));

                if (!toCommitMap.isEmpty()) {
                    toCommitQueue.add(toCommitMap);
                }
            } finally {
                mutex.release();
            }
        } catch (InterruptedException ex) {
            LOGGER.severe(ex.getMessage());
        }
    }

    private Optional<OffsetAndMetadata> getToCommitOffset(TopicPartition topicPartition, long offset) {
        OffsetAndMetadata offsetAndMetadata = null;
        long toCommitOffset = toCommitOffsets.get(topicPartition);
        if (toCommitOffset == offset) {
            Map<Long, Long> uncommitedOffsetRanges = uncommitedOffsetsHandler.getOffsetRanges(topicPartition);
            long greatestOffset = getGreatestReadyToCommitOffset(uncommitedOffsetRanges, offset);

            toCommitOffsets.put(topicPartition, greatestOffset);

            offsetAndMetadata = new OffsetAndMetadata(greatestOffset);
        }
        return Optional.ofNullable(offsetAndMetadata);
    }

    private long getGreatestReadyToCommitOffset(Map<Long, Long> offsetRanges, long firstCommitOffset) {
        long greatestOffset = firstCommitOffset;
        for (; ; ) {
            Long nextOffset = offsetRanges.get(greatestOffset);
            if (nextOffset == null) {
                break;
            }
            offsetRanges.remove(greatestOffset);
            greatestOffset = nextOffset + 1;
        }
        return greatestOffset;
    }
}
