package com.github.mat_sik.kafka_consumer.consumer.offset;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

public class OffsetHandler {

    private final ToCommitOffsetsHandler toCommitOffsetsHandler;
    private final UncommitedOffsetsHandler uncommitedOffsetsHandler;

    public OffsetHandler(UncommitedOffsetsHandler uncommitedOffsetsHandler, ToCommitOffsetsHandler toCommitOffsetsHandler) {
        this.toCommitOffsetsHandler = toCommitOffsetsHandler;
        this.uncommitedOffsetsHandler = uncommitedOffsetsHandler;
    }

    public void registerRecordsAndTryToCommit(ConsumerRecords<String, String> records) {
        Map<TopicPartition, Long> offsets = uncommitedOffsetsHandler.registerRecords(records);
        toCommitOffsetsHandler.tryCommitOffsets(offsets);
    }

}
