package com.rackspace.ceres.app.config;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.config.AbstractReactiveMongoConfiguration;

@Slf4j
@Configuration
public class MongoConfig {
//    @Value("${spring.data.mongodb.uri}")
//    private String mongodbUri;
//
//    @Bean
//    public MongoClient mongoClient() {
//        log.info("mongodb uri: {}", this.mongodbUri);
//        final ConnectionString connectionString = new ConnectionString("mongodb://root:myU8KfkjLC@cassandra-mongodb/ceres?authSource=admin");
//        final MongoClientSettings mongoClientSettings = MongoClientSettings.builder()
//            .applyConnectionString(connectionString)
//            .build();
//        return MongoClients.create(mongoClientSettings);
//    }
//
//    @Override
//    protected String getDatabaseName() {
//        return "ceres";
//    }
//
//    @Override
//    protected boolean autoIndexCreation() {
//        return true;
//    }
}
