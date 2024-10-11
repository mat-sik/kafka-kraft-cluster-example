package com.github.mat_sik.kafka_consumer.client.mongo;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("mongo")
public record MongoClientProperties(
        String host,
        int port,
        String username,
        String password,
        String authDatabase,
        String database) {
}
