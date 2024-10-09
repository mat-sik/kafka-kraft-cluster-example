package com.github.mat_sik.kafka_publisher.client.kafka;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
public class KafkaClientConfiguration {

    @Bean
    public Properties kafkaProducerProperties(KafkaClientConfigurationProperties configurationProperties) {
        var producerProperties = new Properties();

        producerProperties.put(ProducerConfig.CLIENT_ID_CONFIG, configurationProperties.clientId());
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, configurationProperties.hosts());

        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // for maximum consistency. We also get acks=all and enable.indempotence=true by default
        producerProperties.put("min.insync.replicas", "2");

        return producerProperties;
    }

    @Bean
    public Producer<String, String> kafkaProducer(Properties kafkaProducerProperties) {
        return new KafkaProducer<>(kafkaProducerProperties, new StringSerializer(), new StringSerializer());
    }

    @Bean
    public Properties kafkaAdminProperties(KafkaClientConfigurationProperties configurationProperties) {
        var adminProperties = new Properties();

        adminProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, configurationProperties.hosts());

        return adminProperties;
    }

    @Bean
    public Admin kafkaAdmin(Properties kafkaAdminProperties) {
        return Admin.create(kafkaAdminProperties);
    }

}
