package com.github.mat_sik.kafka_producer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Logger;

public class ContinuousProducer {

    private static final Logger LOGGER = Logger.getLogger(ContinuousProducer.class.getName());

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final Producer<String, String> producer;
    private final String topicName;

    public ContinuousProducer(Producer<String, String> producer, String topicName) {
        this.producer = producer;
        this.topicName = topicName;
    }

    public void run() throws ExecutionException, InterruptedException {
        int i = 0;
        for (; ; ) {
            String key = String.valueOf(i);
            String value = getCurrentDateTimeAsString();
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);

            Future<RecordMetadata> future = producer.send(record);

            RecordMetadata metadata = future.get();
            LOGGER.info("### produced record, metadata: " + metadata + " ###");
            i++;
        }
    }

    private String getCurrentDateTimeAsString() {
        LocalDateTime currentDateTime = LocalDateTime.now();
        return FORMATTER.format(currentDateTime);
    }
}
