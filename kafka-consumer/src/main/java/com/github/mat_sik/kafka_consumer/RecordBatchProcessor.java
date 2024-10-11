package com.github.mat_sik.kafka_consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

public class RecordBatchProcessor implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(RecordBatchProcessor.class.getName());

    private final Random random;

    private final BlockingQueue<ConsumerRecords<String, String>> recordBatchQueue;
    private final OffsetCommitHandler offsetCommitHandler;

    public RecordBatchProcessor(
            BlockingQueue<ConsumerRecords<String, String>> recordBatchQueue,
            OffsetCommitHandler offsetCommitHandler
    ) {
        this.recordBatchQueue = recordBatchQueue;
        this.offsetCommitHandler = offsetCommitHandler;

        this.random = new Random();
    }

    @Override
    public void run() {
        try {
            for (; ; ) {
                ConsumerRecords<String, String> recordBatch = recordBatchQueue.take();
                randomlySleep(0.3f);
                logPartitionData(recordBatch);
                offsetCommitHandler.registerBatchAsReadyToBeCommited(recordBatch);
            }
        } catch (InterruptedException ex) {
            LOGGER.info(ex.getMessage());
        }
    }

    private void randomlySleep(float probability) throws InterruptedException {
        if (random.nextFloat() >= 1 - probability) {
            Thread.sleep(Duration.ofSeconds(5).toMillis());
        }
    }

    private void logPartitionData(ConsumerRecords<String, String> records) {
        StringBuilder builder = new StringBuilder("### NEW BATCH");

        Set<TopicPartition> partitions = records.partitions();
        for (TopicPartition partition : partitions) {
            int partitionNumber = partition.partition();

            List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
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
