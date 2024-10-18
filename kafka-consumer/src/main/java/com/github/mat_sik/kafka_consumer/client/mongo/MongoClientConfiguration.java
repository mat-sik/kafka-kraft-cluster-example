package com.github.mat_sik.kafka_consumer.client.mongo;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MongoClientConfiguration {

    @Bean
    public MongoClient mongoClient(MongoClientProperties mongoClientProperties) {
        String connectionString = getConnectionString(mongoClientProperties);
        return MongoClients.create(connectionString);
    }

    private static String getConnectionString(MongoClientProperties mongoClientProperties) {
        return String.format("mongodb://%s:%s@%s:%d/%s",
                mongoClientProperties.username(),
                mongoClientProperties.password(),
                mongoClientProperties.host(),
                mongoClientProperties.port(),
                mongoClientProperties.authDatabase()
        );
    }

    @Bean
    public MongoDatabase mongoDatabase(MongoClient mongoClient, MongoClientProperties mongoClientProperties) {
        return mongoClient.getDatabase(mongoClientProperties.database());
    }

    @Bean
    public MongoCollection<Document> mongoCollection(MongoDatabase mongoDatabase) {
        return mongoDatabase.getCollection("records");
    }

}
