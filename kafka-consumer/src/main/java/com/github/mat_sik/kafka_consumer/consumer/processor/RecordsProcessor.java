package com.github.mat_sik.kafka_consumer.consumer.processor;

import com.github.mat_sik.kafka_consumer.consumer.offset.OffsetHandler;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.ReplaceOptions;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.bson.Document;

import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.logging.Logger;

public class RecordsProcessor implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(RecordsProcessor.class.getName());

    private final BlockingQueue<ConsumerRecords<String, String>> toProcessQueue;
    private final OffsetHandler offsetHandler;

    private final MongoCollection<Document> collection;
    private final Lock readLock;

    public RecordsProcessor(
            BlockingQueue<ConsumerRecords<String, String>> toProcessQueue,
            OffsetHandler offsetHandler,
            Lock readLock,
            MongoCollection<Document> collection
    ) {
        this.toProcessQueue = toProcessQueue;
        this.offsetHandler = offsetHandler;
        this.readLock = readLock;
        this.collection = collection;
    }

    @Override
    public void run() {
        try {
            for (; ; ) {
                readLock.lockInterruptibly();
                try {
                    process();
                } finally {
                    readLock.unlock();
                }
            }
        } catch (InterruptedException ex) {
            LOGGER.info("Got interrupted, exception message: " + ex.getMessage());
        }
    }

    private void process() throws InterruptedException {
        ConsumerRecords<String, String> records = toProcessQueue.take();
        saveAll(records);
        logRecords(records);
        offsetHandler.registerRecordsAndTryToCommit(records);
    }

    private void saveAll(ConsumerRecords<String, String> records) {
        records.forEach(this::saveRecord);
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
