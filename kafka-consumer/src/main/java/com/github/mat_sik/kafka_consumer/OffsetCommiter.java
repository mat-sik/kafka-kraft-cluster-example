package com.github.mat_sik.kafka_consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.logging.Logger;

public class OffsetCommiter {

    private static final Logger LOGGER = Logger.getLogger(OffsetCommiter.class.getName());

    // keeps track of offsets that are ready to be commited per partition.
    private final Map<TopicPartition, Map<Long, Long>> uncommitedOffsets;

    // keeps tack of offsets that need to be commited per partition.
    private final Map<TopicPartition, Long> toCommitOffsets;

    private final ConcurrentLinkedQueue<Map<TopicPartition, OffsetAndMetadata>> commitQueue;
    private final Semaphore mutex;

    public OffsetCommiter(
            Map<TopicPartition, OffsetAndMetadata> commited,
            ConcurrentLinkedQueue<Map<TopicPartition, OffsetAndMetadata>> commitQueue
    ) {
        this.uncommitedOffsets = createUncommitedOffsetsTracker(commited.keySet());
        this.toCommitOffsets = createToCommitOffsetsTracker(commited);
        this.commitQueue = commitQueue;
        this.mutex = new Semaphore(1);
    }

    private static Map<TopicPartition, Map<Long, Long>> createUncommitedOffsetsTracker(Set<TopicPartition> topicPartitions) {
        Map<TopicPartition, Map<Long, Long>> partitionsOffsetTracker = new ConcurrentHashMap<>();

        topicPartitions.forEach(topicPartition -> {
            Map<Long, Long> partitionOffsetTracker = new ConcurrentHashMap<>();
            partitionsOffsetTracker.put(topicPartition, partitionOffsetTracker);
        });

        return partitionsOffsetTracker;
    }

    private static Map<TopicPartition, Long> createToCommitOffsetsTracker(Map<TopicPartition, OffsetAndMetadata> commited) {
        Map<TopicPartition, Long> commitTracker = new HashMap<>();

        commited.forEach((topicPartition, offsetAndMetadata) -> {
            long offset = offsetAndMetadata == null ? 0 : offsetAndMetadata.offset();
            commitTracker.put(topicPartition, offset);
        });

        return commitTracker;
    }

    public void registerBatchAsReadyToBeCommited(ConsumerRecords<String, String> records) {
        Map<TopicPartition, Long> initialOffsets = new HashMap<>();

        Set<TopicPartition> topicPartitions = records.partitions();
        topicPartitions.forEach(topicPartition -> {
            List<ConsumerRecord<String, String>> partitionRecords = records.records(topicPartition);

            Map<Long, Long> partitionOffsetTracker = uncommitedOffsets.get(topicPartition);
            if (partitionOffsetTracker == null) {
                // todo: update commit tracker under mutex
                throw new RuntimeException("Unregistered partition number.");
            }

            ConsumerRecord<String, String> firstRecord = partitionRecords.getFirst();
            long firstOffset = firstRecord.offset();

            ConsumerRecord<String, String> lastRecord = partitionRecords.getLast();
            long lastOffset = lastRecord.offset();

            partitionOffsetTracker.put(firstOffset, lastOffset);

            initialOffsets.put(topicPartition, firstOffset);
        });

        tryCommitOffsets(initialOffsets);
    }

    private void tryCommitOffsets(Map<TopicPartition, Long> initialOffsets) {
        try {
            mutex.acquire();
            try {
                Map<TopicPartition, OffsetAndMetadata> toCommitMap = new HashMap<>();
                initialOffsets.forEach(((topicPartition, initialOffset) -> {
                    prepareToCommitOffsets(toCommitMap, topicPartition, initialOffset);
                }));
                if (!toCommitMap.isEmpty()) {
                    commitQueue.add(toCommitMap);
                }
            } finally {
                mutex.release();
            }
        } catch (InterruptedException ex) {
            LOGGER.severe(ex.getMessage());
        }
    }

    private void prepareToCommitOffsets(
            Map<TopicPartition, OffsetAndMetadata> commitMap,
            TopicPartition topicPartition,
            long offset
    ) {
        long toCommitOffset = toCommitOffsets.get(topicPartition);
        if (toCommitOffset == offset) {
            Map<Long, Long> partitionUncommitedOffsets = uncommitedOffsets.get(topicPartition);
            long newToCommitOffset = getLastReadyToCommitOffset(partitionUncommitedOffsets, offset);

            toCommitOffsets.put(topicPartition, newToCommitOffset);

            OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(newToCommitOffset);
            commitMap.put(topicPartition, offsetAndMetadata);
        }
    }

    private long getLastReadyToCommitOffset(Map<Long, Long> partitionOffsetTracker, long firstCommitOffset) {
        long lastReadyToCommitOffset = firstCommitOffset;
        for (; ; ) {
            Long nextOffset = partitionOffsetTracker.get(lastReadyToCommitOffset);
            if (nextOffset == null) {
                break;
            }
            partitionOffsetTracker.remove(lastReadyToCommitOffset);
            lastReadyToCommitOffset = nextOffset + 1;
        }
        return lastReadyToCommitOffset;
    }

}
