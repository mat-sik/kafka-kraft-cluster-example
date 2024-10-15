package com.github.mat_sik.kafka_consumer.consumer;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ToCommitQueueHandler {

    private final ConcurrentLinkedQueue<Map<TopicPartition, OffsetAndMetadata>> toCommitQueue;

    public ToCommitQueueHandler(ConcurrentLinkedQueue<Map<TopicPartition, OffsetAndMetadata>> toCommitQueue) {
        this.toCommitQueue = toCommitQueue;
    }

    public boolean add(Map<TopicPartition, OffsetAndMetadata> toCommitOffsets) {
        return toCommitQueue.add(toCommitOffsets);
    }

    public Optional<Map<TopicPartition, OffsetAndMetadata>> poolReadyToCommitOffsets() {
        Map<TopicPartition, OffsetAndMetadata> compactedOffsets = compactReadyToCommitOffsets(toCommitQueue);

        if (compactedOffsets.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(compactedOffsets);
    }

    private static Map<TopicPartition, OffsetAndMetadata> compactReadyToCommitOffsets(
            ConcurrentLinkedQueue<Map<TopicPartition, OffsetAndMetadata>> toCommitQueue
    ) {
        Map<TopicPartition, OffsetAndMetadata> compactedOffsets = new HashMap<>();

        Map<TopicPartition, OffsetAndMetadata> currentOffsets = toCommitQueue.poll();
        while (currentOffsets != null) {
            mergeWithHigherOffsets(compactedOffsets, currentOffsets);
            currentOffsets = toCommitQueue.poll();
        }

        return compactedOffsets;
    }

    private static void mergeWithHigherOffsets(
            Map<TopicPartition, OffsetAndMetadata> collector,
            Map<TopicPartition, OffsetAndMetadata> current
    ) {
        current.forEach(((topicPartition, offsetAndMetadata) ->
                updateToHigherOffset(collector, topicPartition, offsetAndMetadata)));
    }

    private static void updateToHigherOffset(
            Map<TopicPartition, OffsetAndMetadata> collector,
            TopicPartition topicPartition,
            OffsetAndMetadata offsetAndMetadata
    ) {
        collector.compute(topicPartition, (_, persistedOffsetAndMetadata) -> {
            if (persistedOffsetAndMetadata == null) {
                return offsetAndMetadata;
            }
            if (offsetAndMetadata.offset() > persistedOffsetAndMetadata.offset()) {
                return offsetAndMetadata;
            }
            return persistedOffsetAndMetadata;
        });
    }

}
