package com.github.mat_sik.kafka_consumer.consumer.offset;

import com.github.mat_sik.kafka_consumer.consumer.ToCommitQueueHandler;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Set;

public class OffsetHandler {

    private final ToCommitOffsetsHandler toCommitOffsetsHandler;
    private final UncommitedOffsetsHandler uncommitedOffsetsHandler;

    private OffsetHandler(UncommitedOffsetsHandler uncommitedOffsetsHandler, ToCommitOffsetsHandler toCommitOffsetsHandler) {
        this.toCommitOffsetsHandler = toCommitOffsetsHandler;
        this.uncommitedOffsetsHandler = uncommitedOffsetsHandler;
    }

    public static OffsetHandler create(
            Map<TopicPartition, OffsetAndMetadata> committed,
            ToCommitQueueHandler toCommitQueueHandler
    ) {
        Set<TopicPartition> topicPartitions = committed.keySet();

        var uncommitedOffsetsHandler = new UncommitedOffsetsHandler(topicPartitions);
        var toCommitOffsetsHandler = new ToCommitOffsetsHandler(committed, toCommitQueueHandler, uncommitedOffsetsHandler);

        return new OffsetHandler(uncommitedOffsetsHandler, toCommitOffsetsHandler);
    }

    public void registerRecordsAndTryToCommit(ConsumerRecords<String, String> records) {
        Map<TopicPartition, Long> offsets = uncommitedOffsetsHandler.registerRecords(records);
        toCommitOffsetsHandler.tryCommitOffsets(offsets);
    }

}
