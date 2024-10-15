package com.github.mat_sik.kafka_consumer.consumer.offset;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

public class OffsetHandler {

    private final ToCommitOffsetsHandler toCommitOffsetsHandler;
    private final UncommitedOffsetsHandler uncommitedOffsetsHandler;

    private OffsetHandler(UncommitedOffsetsHandler uncommitedOffsetsHandler, ToCommitOffsetsHandler toCommitOffsetsHandler) {
        this.toCommitOffsetsHandler = toCommitOffsetsHandler;
        this.uncommitedOffsetsHandler = uncommitedOffsetsHandler;
    }

    public static OffsetHandler create(
            Map<TopicPartition, OffsetAndMetadata> committed,
            ConcurrentLinkedQueue<Map<TopicPartition, OffsetAndMetadata>> toCommitQueue
    ) {
        Set<TopicPartition> topicPartitions = committed.keySet();

        var uncommitedOffsetsHandler = new UncommitedOffsetsHandler(topicPartitions);
        var toCommitOffsetsHandler = new ToCommitOffsetsHandler(committed, toCommitQueue, uncommitedOffsetsHandler);

        return new OffsetHandler(uncommitedOffsetsHandler, toCommitOffsetsHandler);
    }

    public void registerRecordsAndTryToCommit(ConsumerRecords<String, String> records) {
        Map<TopicPartition, Long> offsets = uncommitedOffsetsHandler.registerRecords(records);
        toCommitOffsetsHandler.tryCommitOffsets(offsets);
    }

}
