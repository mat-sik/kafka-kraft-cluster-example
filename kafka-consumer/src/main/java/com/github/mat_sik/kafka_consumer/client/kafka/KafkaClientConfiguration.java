package com.github.mat_sik.kafka_consumer.client.kafka;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
public class KafkaClientConfiguration {

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

    @Bean
    public Properties kafkaConsumerProperties(KafkaClientConfigurationProperties configurationProperties) {
        var consumerProperties = new Properties();

        consumerProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, configurationProperties.clientId());
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, configurationProperties.hosts());

        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, configurationProperties.groupId());

        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        return consumerProperties;
    }

}
