package com.github.mat_sik.kafka_consumer.consumer;

import com.github.mat_sik.kafka_consumer.consumer.controller.ProcessorsProcessingController;
import com.github.mat_sik.kafka_consumer.consumer.offset.OffsetHandler;
import com.github.mat_sik.kafka_consumer.consumer.processor.RecordsProcessor;
import com.mongodb.client.MongoCollection;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.bson.Document;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

public class ContinuousConsumer implements Runnable {

    private static final Logger LOGGER = Logger.getLogger(ContinuousConsumer.class.getName());
    private static final int PROCESSOR_AMOUNT = 3;

    private static final Duration POLL_DURATION = Duration.ofMillis(100);

    private final Consumer<String, String> consumer;
    private final Collection<String> topicNames;

    private final BlockingQueue<ConsumerRecords<String, String>> toProcessQueue;
    private final ToCommitQueueHandler toCommitQueueHandler;
    private final OffsetHandler offsetHandler;
    private final ContinuousConsumerRebalanceListener continuousConsumerRebalanceListener;
    private final ProcessorsProcessingController processingController;

    private final MongoCollection<Document> collection;

    private final List<Thread> processors;

    public ContinuousConsumer(
            Consumer<String, String> consumer,
            Collection<String> topicNames,
            ContinuousConsumerRebalanceListener continuousConsumerRebalanceListener, BlockingQueue<ConsumerRecords<String, String>> toProcessQueue,
            ToCommitQueueHandler toCommitQueueHandler,
            OffsetHandler offsetHandler,
            ProcessorsProcessingController processingController,
            MongoCollection<Document> collection
    ) {
        this.consumer = consumer;
        this.topicNames = topicNames;
        this.toProcessQueue = toProcessQueue;
        this.toCommitQueueHandler = toCommitQueueHandler;
        this.offsetHandler = offsetHandler;
        this.continuousConsumerRebalanceListener = continuousConsumerRebalanceListener;
        this.processingController = processingController;
        this.collection = collection;
        this.processors = new ArrayList<>();
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(topicNames, continuousConsumerRebalanceListener);
            continuousConsume();
        } catch (WakeupException ex) {
            LOGGER.info(ex.getMessage());
        } catch (Exception ex) {
            LOGGER.severe(ex.getMessage());
        } finally {
            consumer.close();
        }
    }

    private void continuousConsume() {
        try {
            setUpProcessors();
            for (; ; ) {
                // After wakeup() was called on consumer, poll() will throw WakeupException.
                ConsumerRecords<String, String> records = consumer.poll(POLL_DURATION);

                if (!records.isEmpty()) {
                    toProcessQueue.put(records);
                }

                Optional<Map<TopicPartition, OffsetAndMetadata>> readyToCommitOffsets = toCommitQueueHandler.poolReadyToCommitOffsets();
                readyToCommitOffsets.ifPresent(toCommitOffsets -> {
                    logToCommitMap(toCommitOffsets);
                    consumer.commitSync(toCommitOffsets);
                });
            }
        } catch (InterruptedException ex) {
            LOGGER.severe("Got interrupted, exception message: " + ex.getMessage());
            shutdown();
        }
    }

    private void setUpProcessors() {
        for (int i = 0; i < PROCESSOR_AMOUNT; i++) {
            String name = String.format("processor-%d", i);
            Thread thread = Thread.ofVirtual()
                    .name(name)
                    .start(new RecordsProcessor(toProcessQueue, offsetHandler, processingController, collection));
            processors.add(thread);
        }
    }

    private void logToCommitMap(Map<TopicPartition, OffsetAndMetadata> toCommitMap) {
        StringBuilder builder = new StringBuilder("## TO COMMIT MAP:");
        toCommitMap.forEach(((topicPartition, offsetAndMetadata) -> {
            builder.append(" | PARTITION: ")
                    .append(topicPartition.partition())
                    .append(" OFFSET: ")
                    .append(offsetAndMetadata.offset());
        }));

        builder.append(" | ##");

        LOGGER.info(builder.toString());
    }

    public void shutdown() {
        for (Thread processor : processors) {
            processor.interrupt();
        }
        consumer.wakeup();
    }

}
