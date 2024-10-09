package com.github.mat_sik.kafka_producer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ContinuousProducer {

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final Duration SLEEP_DURATION = Duration.ofMillis(100);

    private final Producer<String, String> producer;
    private final String topicName;

    public ContinuousProducer(Producer<String, String> producer, String topicName) {
        this.producer = producer;
        this.topicName = topicName;
    }

    public void run() throws ExecutionException, InterruptedException {
        for (; ; ) {
            String key = getCurrentDateTimeAsString();
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, key);

            Future<RecordMetadata> future = producer.send(record);

            future.get();

            Thread.sleep(SLEEP_DURATION);
        }
    }

    private String getCurrentDateTimeAsString() {
        LocalDateTime currentDateTime = LocalDateTime.now();
        return FORMATTER.format(currentDateTime);
    }
}
