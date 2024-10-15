package com.github.mat_sik.kafka_consumer.consumer.offset;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class UncommitedOffsetsHandler {

    private final Map<TopicPartition, Map<Long, Long>> uncommitedOffsets;

    public UncommitedOffsetsHandler(Set<TopicPartition> topicPartitions) {
        this.uncommitedOffsets = createUncommitedOffsetsTracker(topicPartitions);
    }

    private static Map<TopicPartition, Map<Long, Long>> createUncommitedOffsetsTracker(Set<TopicPartition> topicPartitions) {
        Map<TopicPartition, Map<Long, Long>> uncommitedOffsets = new ConcurrentHashMap<>();

        topicPartitions.forEach(topicPartition -> {
            Map<Long, Long> offsetRanges = new ConcurrentHashMap<>();
            uncommitedOffsets.put(topicPartition, offsetRanges);
        });

        return uncommitedOffsets;
    }

    public Map<Long, Long> getOffsetRanges(TopicPartition topicPartition) {
        return uncommitedOffsets.get(topicPartition);
    }

    public Map<TopicPartition, Long> registerRecords(ConsumerRecords<String, String> records) {
        Map<TopicPartition, Long> firstOffsets = new HashMap<>();

        Set<TopicPartition> topicPartitions = records.partitions();
        topicPartitions.forEach(topicPartition -> {
            Map<Long, Long> offsetTracker = uncommitedOffsets.get(topicPartition);
            List<ConsumerRecord<String, String>> batch = records.records(topicPartition);

            long firstOffset = registerBatch(offsetTracker, batch);

            firstOffsets.put(topicPartition, firstOffset);
        });

        return firstOffsets;
    }

    private static long registerBatch(Map<Long, Long> offsetTracker, List<ConsumerRecord<String, String>> batch) {
        if (offsetTracker == null) {
            // todo: update commit tracker under mutex
            throw new RuntimeException("Unregistered partition number.");
        }

        long firstOffset = batch.getFirst().offset();
        long lastOffset = batch.getLast().offset();
        offsetTracker.put(firstOffset, lastOffset);

        return firstOffset;
    }
}
