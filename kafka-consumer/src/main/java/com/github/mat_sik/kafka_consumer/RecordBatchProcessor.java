package com.github.mat_sik.kafka_consumer;

import com.mongodb.client.MongoCollection;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.bson.Document;

import javax.print.Doc;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

public class RecordBatchProcessor implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(RecordBatchProcessor.class.getName());

    private final BlockingQueue<ConsumerRecords<String, String>> toProcessQueue;
    private final OffsetCommitHandler offsetCommitHandler;

    private final MongoCollection<Document> collection;

    public RecordBatchProcessor(
            BlockingQueue<ConsumerRecords<String, String>> toProcessQueue,
            OffsetCommitHandler offsetCommitHandler,
            MongoCollection<Document> collection
    ) {
        this.toProcessQueue = toProcessQueue;
        this.offsetCommitHandler = offsetCommitHandler;
        this.collection = collection;
    }

    @Override
    public void run() {
        try {
            for (; ; ) {
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }

                ConsumerRecords<String, String> records = toProcessQueue.take();
                logRecords(records);
                saveAll(records);
                offsetCommitHandler.registerRecordsAndTryToCommit(records);
            }
        } catch (InterruptedException ex) {
            LOGGER.info("Got interrupted, exception message: " + ex.getMessage());
        }
    }

    private void saveAll(ConsumerRecords<String, String> records) {
        records.forEach(this::saveRecord);
    }

    private void saveRecord(ConsumerRecord<String, String> record) {
        Document doc = new Document()
                .append("_id", record.key())
                .append("value", record.value())
                .append("offset", record.offset());

        collection.insertOne(doc);
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
