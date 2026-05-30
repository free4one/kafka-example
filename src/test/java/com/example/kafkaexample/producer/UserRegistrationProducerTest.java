package com.example.kafkaexample.producer;

import com.example.kafkaexample.dto.UserRegistrationEvent;
import com.example.kafkaexample.repository.UserRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@EmbeddedKafka(
        partitions = 1,
        topics = {"user-registration-topic", "user-registration-dlt"}
)
@TestPropertySource(properties = {
        "spring.autoconfigure.exclude=" +
                "org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration," +
                "org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration," +
                "org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration," +
                "org.springframework.boot.autoconfigure.data.jpa.JpaRepositoriesAutoConfiguration",
        "kafka.topic.user-registration=user-registration-topic",
        "kafka.topic.user-registration-dlt=user-registration-dlt",
        "kafka.consumer.group-id=test-group",
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "spring.kafka.listener.auto-startup=false"
})
class UserRegistrationProducerTest {

    @MockitoBean
    private UserRepository userRepository;

    @Autowired
    private UserRegistrationProducer producer;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    private static final int POLL_TIMEOUT_SECONDS = 10;

    @Test
    @DisplayName("이벤트 발행 시 Kafka 토픽에 email을 파티션 키로 메시지 전송")
    void send_publishesMessageWithEmailAsKey() throws Exception {
        UserRegistrationEvent event = new UserRegistrationEvent(
                UUID.randomUUID().toString(),
                "producer@example.com",
                "테스터",
                Instant.now()
        );

        producer.send(event);

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("producer-test-group", "true", embeddedKafkaBroker);
        consumerProps.put("key.deserializer", StringDeserializer.class);
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        ConsumerFactory<String, byte[]> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps);
        try (Consumer<String, byte[]> consumer = consumerFactory.createConsumer()) {
            embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, "user-registration-topic");
            ConsumerRecord<String, byte[]> record = KafkaTestUtils.getSingleRecord(
                    consumer, "user-registration-topic", Duration.ofSeconds(POLL_TIMEOUT_SECONDS));

            assertThat(record.key()).isEqualTo("producer@example.com");

            // Parse JSON from byte array to verify content
            var objectMapper = new ObjectMapper();
            objectMapper.findAndRegisterModules();
            var eventFromRecord = objectMapper.readValue(record.value(), UserRegistrationEvent.class);
            assertThat(eventFromRecord.email()).isEqualTo("producer@example.com");
            assertThat(eventFromRecord.username()).isEqualTo("테스터");
        }
    }

}
