package com.example.kafkaexample.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Value("${kafka.topic.user-registration}")
    private String userRegistrationTopic;

    @Value("${kafka.topic.user-registration-dlt}")
    private String userRegistrationDltTopic;

    @Bean
    public NewTopic userRegistrationTopic() {
        return TopicBuilder.name(userRegistrationTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic userRegistrationDltTopic() {
        return TopicBuilder.name(userRegistrationDltTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
