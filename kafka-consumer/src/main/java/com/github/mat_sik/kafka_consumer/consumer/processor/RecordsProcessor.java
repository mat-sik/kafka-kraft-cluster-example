package com.github.mat_sik.kafka_consumer.consumer.processor;

import com.github.mat_sik.kafka_consumer.consumer.controller.ProcessorsProcessingController;
import com.github.mat_sik.kafka_consumer.consumer.offset.OffsetHandler;
import com.github.mat_sik.kafka_consumer.consumer.offset.OffsetRange;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.ReplaceOptions;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.bson.Document;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

public class RecordsProcessor implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(RecordsProcessor.class.getName());

    private final BlockingQueue<ConsumerRecords<String, String>> toProcessQueue;
    private final OffsetHandler offsetHandler;

    private final MongoCollection<Document> collection;
    private final ProcessorsProcessingController processingController;

    public RecordsProcessor(
            BlockingQueue<ConsumerRecords<String, String>> toProcessQueue,
            OffsetHandler offsetHandler,
            ProcessorsProcessingController processingController,
            MongoCollection<Document> collection
    ) {
        this.toProcessQueue = toProcessQueue;
        this.offsetHandler = offsetHandler;
        this.processingController = processingController;
        this.collection = collection;
    }

    @Override
    public void run() {
        try {
            for (; ; ) {
                ConsumerRecords<String, String> records = toProcessQueue.take();
                processingController.lock();
                try {
                    processRecords(records);
                    logRecords(records);
                } finally {
                    processingController.unlock();
                }
            }
        } catch (InterruptedException ex) {
            LOGGER.info("Got interrupted, exception message: " + ex.getMessage());
        }
    }

    private void processRecords(ConsumerRecords<String, String> records) {
        Map<TopicPartition, Long> firstOffsets = new HashMap<>();

        Set<TopicPartition> topicPartitions = records.partitions();
        topicPartitions.forEach(topicPartition -> {
            if (!processingController.shouldProcess() || !offsetHandler.isTopicPartitionRegistered(topicPartition)) {
                return;
            }
            List<ConsumerRecord<String, String>> batch = records.records(topicPartition);
            OffsetRange offsetRange = processBatch(batch);

            OptionalLong firstPersistedOffset = offsetHandler.registerBatch(topicPartition, offsetRange);
            firstPersistedOffset.ifPresent(offset -> firstOffsets.put(topicPartition, offset));
        });

        if (!firstOffsets.isEmpty()) {
            offsetHandler.tryCommitOffsets(firstOffsets);
        }
    }

    private OffsetRange processBatch(List<ConsumerRecord<String, String>> batch) {
        long firstOffset = -1;
        long lastOffset = -1;

        for (ConsumerRecord<String, String> record : batch) {
            if (!processingController.shouldProcess()) {
                break;
            }

            saveRecord(record);

            long offset = record.offset();
            if (firstOffset == -1) {
                firstOffset = offset;
            }
            lastOffset = offset;
        }

        return new OffsetRange(firstOffset, lastOffset);
    }

    private void saveRecord(ConsumerRecord<String, String> record) {
        Integer key = Integer.valueOf(record.key());
        Document id = new Document("_id", key);
        Document doc = new Document()
                .append("_id", key)
                .append("value", record.value())
                .append("partition", record.partition())
                .append("offset", record.offset());

        collection.replaceOne(id, doc, new ReplaceOptions().upsert(true));
    }

    private void logRecords(ConsumerRecords<String, String> records) {
        StringBuilder builder = new StringBuilder("### NEW RECORDS");

        Set<TopicPartition> topicPartitions = records.partitions();
        topicPartitions.forEach(topicPartition -> {
            int partitionNumber = topicPartition.partition();

            if (!offsetHandler.isTopicPartitionRegistered(topicPartition)) {
                builder.append(" | PARTITION: ").append(partitionNumber).append(" SKIPPED");
                return;
            }

            List<ConsumerRecord<String, String>> partitionRecords = records.records(topicPartition);
            long firstOffset = partitionRecords.getFirst().offset();
            long lastOffset = partitionRecords.getLast().offset();

            builder.append(" | PARTITION: ")
                    .append(partitionNumber)
                    .append(" FIRST OFFSET: ")
                    .append(firstOffset)
                    .append(" LAST OFFSET: ")
                    .append(lastOffset);
        });

        builder.append(" | ###");

        LOGGER.info(builder.toString());
    }

}
