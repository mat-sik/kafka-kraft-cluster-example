package com.github.mat_sik.kafka_consumer.consumer.offset;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

    public Map<TopicPartition, Long> registerRecords(ConsumerRecords<String, String> records) {
        Map<TopicPartition, Long> firstOffsets = new HashMap<>();

        Set<TopicPartition> topicPartitions = records.partitions();
        topicPartitions.forEach(topicPartition -> {
            Optional<Map<Long, Long>> uncommittedOffsetRanges = getOffsetRanges(topicPartition);
            uncommittedOffsetRanges.ifPresent(offsetRanges -> {
                List<ConsumerRecord<String, String>> batch = records.records(topicPartition);

                long firstOffset = registerBatch(offsetRanges, batch);

                firstOffsets.put(topicPartition, firstOffset);
            });
        });

        return firstOffsets;
    }

    public Optional<Map<Long, Long>> getOffsetRanges(TopicPartition topicPartition) {
        Map<Long, Long> offsetRanges = uncommittedOffsets.get(topicPartition);
        if (offsetRanges == null) {
            LOGGER.severe("Unregistered TopicPartition");
        }
        return Optional.ofNullable(offsetRanges);
    }

    private static long registerBatch(Map<Long, Long> offsetRanges, List<ConsumerRecord<String, String>> batch) {
        long firstOffset = batch.getFirst().offset();
        long lastOffset = batch.getLast().offset();

        offsetRanges.put(firstOffset, lastOffset);

        return firstOffset;
    }
}
