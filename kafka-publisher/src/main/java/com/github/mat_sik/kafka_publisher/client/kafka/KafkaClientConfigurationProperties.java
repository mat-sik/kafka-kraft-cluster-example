package com.github.mat_sik.kafka_publisher.client.kafka;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("kafka")
public record KafkaClientConfigurationProperties(String clientId, String hosts) {
}
