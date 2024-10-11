package com.github.mat_sik.kafka_consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

public class RecordBatchProcessor implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(RecordBatchProcessor.class.getName());

    private final BlockingQueue<ConsumerRecords<String, String>> toProcessQueue;
    private final OffsetCommitHandler offsetCommitHandler;

    public RecordBatchProcessor(
            BlockingQueue<ConsumerRecords<String, String>> toProcessQueue,
            OffsetCommitHandler offsetCommitHandler
    ) {
        this.toProcessQueue = toProcessQueue;
        this.offsetCommitHandler = offsetCommitHandler;
    }

    @Override
    public void run() {
        try {
            for (; ; ) {
                ConsumerRecords<String, String> records = toProcessQueue.take();
                logRecords(records);
                offsetCommitHandler.registerRecordsAndTryToCommit(records);
            }
        } catch (InterruptedException ex) {
            LOGGER.info(ex.getMessage());
        }
    }

    private void logRecords(ConsumerRecords<String, String> records) {
        StringBuilder builder = new StringBuilder("### NEW RECORDS");

        Set<TopicPartition> topicPartitions = records.partitions();
        for (TopicPartition topicPartition : topicPartitions) {
            int partitionNumber = topicPartition.partition();

            List<ConsumerRecord<String, String>> partitionRecords = records.records(topicPartition);
            long firstOffset = partitionRecords.getFirst().offset();
            long lastOffset = partitionRecords.getLast().offset();

            builder.append(" | PARTITION: ")
                    .append(partitionNumber)
                    .append(" FIRST OFFSET: ")
                    .append(firstOffset)
                    .append(" LAST OFFSET: ")
                    .append(lastOffset);
        }

        builder.append(" | ###");

        LOGGER.info(builder.toString());
    }

}
