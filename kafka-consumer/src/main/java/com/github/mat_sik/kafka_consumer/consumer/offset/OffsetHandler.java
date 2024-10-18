package com.github.mat_sik.kafka_consumer.consumer.offset;

import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.OptionalLong;

public class OffsetHandler {

    private final ToCommitOffsetsHandler toCommitOffsetsHandler;
    private final UncommitedOffsetsHandler uncommitedOffsetsHandler;

    public OffsetHandler(UncommitedOffsetsHandler uncommitedOffsetsHandler, ToCommitOffsetsHandler toCommitOffsetsHandler) {
        this.toCommitOffsetsHandler = toCommitOffsetsHandler;
        this.uncommitedOffsetsHandler = uncommitedOffsetsHandler;
    }

    public OptionalLong registerBatch(TopicPartition topicPartition, OffsetRange offsetRange) {
        return uncommitedOffsetsHandler.registerBatch(topicPartition, offsetRange);
    }

    public void tryCommitOffsets(Map<TopicPartition, Long> toCommitOffsets) {
        toCommitOffsetsHandler.tryCommitOffsets(toCommitOffsets);
    }

    public boolean isTopicPartitionRegistered(TopicPartition topicPartition) {
        return uncommitedOffsetsHandler.isTopicPartitionRegistered(topicPartition);
    }

}
