package com.github.mat_sik.kafka_consumer.consumer.offset;

import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

public class UncommitedOffsetsHandler {

    private static final Logger LOGGER = Logger.getLogger(UncommitedOffsetsHandler.class.getName());

    private final Map<TopicPartition, Map<Long, Long>> uncommittedOffsets;

    public UncommitedOffsetsHandler() {
        this.uncommittedOffsets = new ConcurrentHashMap<>();
    }

    public void updateUncommittedOffsets(Set<TopicPartition> topicPartitions) {
        uncommittedOffsets.clear();
        populateUncommittedOffsets(uncommittedOffsets, topicPartitions);
    }

    private static void populateUncommittedOffsets(
            Map<TopicPartition, Map<Long, Long>> uncommitedOffsets,
            Set<TopicPartition> topicPartitions
    ) {
        topicPartitions.forEach(topicPartition -> {
            Map<Long, Long> offsetRanges = new ConcurrentHashMap<>();
            uncommitedOffsets.put(topicPartition, offsetRanges);
        });
    }

    public boolean isTopicPartitionRegistered(TopicPartition topicPartition) {
        return uncommittedOffsets.containsKey(topicPartition);
    }

    public OptionalLong registerBatch(TopicPartition topicPartition, OffsetRange offsetRange) {
        Optional<Map<Long, Long>> uncommittedOffsetRanges = getOffsetRanges(topicPartition);
        if (uncommittedOffsetRanges.isEmpty()) {
            return OptionalLong.empty();
        }
        Map<Long, Long> offsetRanges = uncommittedOffsetRanges.get();

        offsetRanges.put(offsetRange.first(), offsetRange.last());

        return OptionalLong.of(offsetRange.first());
    }

    public Optional<Map<Long, Long>> getOffsetRanges(TopicPartition topicPartition) {
        Map<Long, Long> offsetRanges = uncommittedOffsets.get(topicPartition);
        if (offsetRanges == null) {
            LOGGER.severe("Unregistered TopicPartition");
        }
        return Optional.ofNullable(offsetRanges);
    }
}
