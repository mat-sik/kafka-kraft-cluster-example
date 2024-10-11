package com.github.mat_sik.kafka_consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.logging.Logger;

public class OffsetCommitHandler {

    private static final Logger LOGGER = Logger.getLogger(OffsetCommitHandler.class.getName());

    // keeps track of offsets that are ready to be commited per partition.
    private final Map<TopicPartition, Map<Long, Long>> uncommitedOffsets;

    // keeps tack of offsets that need to be commited per partition.
    private final Map<TopicPartition, Long> toCommitOffsets;

    private final ConcurrentLinkedQueue<Map<TopicPartition, OffsetAndMetadata>> toCommitQueue;
    private final Semaphore mutex;

    public OffsetCommitHandler(
            Map<TopicPartition, OffsetAndMetadata> commited,
            ConcurrentLinkedQueue<Map<TopicPartition, OffsetAndMetadata>> toCommitQueue
    ) {
        this.uncommitedOffsets = createUncommitedOffsetsTracker(commited.keySet());
        this.toCommitOffsets = createToCommitOffsetsTracker(commited);
        this.toCommitQueue = toCommitQueue;
        this.mutex = new Semaphore(1);
    }

    private static Map<TopicPartition, Map<Long, Long>> createUncommitedOffsetsTracker(Set<TopicPartition> topicPartitions) {
        Map<TopicPartition, Map<Long, Long>> uncommitedOffsets = new ConcurrentHashMap<>();

        topicPartitions.forEach(topicPartition -> {
            Map<Long, Long> offsetRanges = new ConcurrentHashMap<>();
            uncommitedOffsets.put(topicPartition, offsetRanges);
        });

        return uncommitedOffsets;
    }

    private static Map<TopicPartition, Long> createToCommitOffsetsTracker(Map<TopicPartition, OffsetAndMetadata> commited) {
        Map<TopicPartition, Long> toCommitOffsets = new HashMap<>();

        commited.forEach((topicPartition, offsetAndMetadata) -> {
            long offset = offsetAndMetadata == null ? 0 : offsetAndMetadata.offset();
            toCommitOffsets.put(topicPartition, offset);
        });

        return toCommitOffsets;
    }

    public void registerRecordsAndTryToCommit(ConsumerRecords<String, String> records) {
        Map<TopicPartition, Long> offsets = registerRecords(records);
        tryCommitOffsets(offsets);
    }

    public Map<TopicPartition, Long> registerRecords(ConsumerRecords<String, String> records) {
        Map<TopicPartition, Long> initialOffsets = new HashMap<>();

        Set<TopicPartition> topicPartitions = records.partitions();
        topicPartitions.forEach(topicPartition -> {
            Map<Long, Long> offsetTracker = uncommitedOffsets.get(topicPartition);
            List<ConsumerRecord<String, String>> batch = records.records(topicPartition);

            long firstOffset = registerBatch(offsetTracker, batch);

            initialOffsets.put(topicPartition, firstOffset);
        });

        return initialOffsets;
    }

    private static long registerBatch(
            Map<Long, Long> offsetTracker,
            List<ConsumerRecord<String, String>> batch
    ) {
        if (offsetTracker == null) {
            // todo: update commit tracker under mutex
            throw new RuntimeException("Unregistered partition number.");
        }

        long firstOffset = batch.getFirst().offset();
        long lastOffset = batch.getLast().offset();
        offsetTracker.put(firstOffset, lastOffset);

        return firstOffset;
    }

    private void tryCommitOffsets(Map<TopicPartition, Long> offsets) {
        try {
            mutex.acquire();
            try {
                Map<TopicPartition, OffsetAndMetadata> toCommitMap = new HashMap<>();

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
            Map<Long, Long> uncommitedOffsetRanges = uncommitedOffsets.get(topicPartition);
            long greatestOffset = getGreatestReadyToCommitOffset(uncommitedOffsetRanges, offset);

            toCommitOffsets.put(topicPartition, greatestOffset);

            offsetAndMetadata = new OffsetAndMetadata(greatestOffset);
        }
        return Optional.ofNullable(offsetAndMetadata);
    }

    private long getGreatestReadyToCommitOffset(Map<Long, Long> partitionOffsetTracker, long firstCommitOffset) {
        long greatestOffset = firstCommitOffset;
        for (; ; ) {
            Long nextOffset = partitionOffsetTracker.get(greatestOffset);
            if (nextOffset == null) {
                break;
            }
            partitionOffsetTracker.remove(greatestOffset);
            greatestOffset = nextOffset + 1;
        }
        return greatestOffset;
    }

}
