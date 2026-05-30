package com.example.kafkaexample.producer;

import com.example.kafkaexample.dto.UserRegistrationEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class UserRegistrationProducerUnitTest {

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    private UserRegistrationProducer producer;

    @BeforeEach
    void setUp() {
        producer = new UserRegistrationProducer(kafkaTemplate);
        ReflectionTestUtils.setField(producer, "topic", "user-registration-topic");
    }

    @Test
    @DisplayName("Kafka 전송 실패 시 예외를 삼키고 에러 로그만 출력")
    void send_whenKafkaFails_doesNotThrow() {
        CompletableFuture<SendResult<String, Object>> failedFuture =
                CompletableFuture.failedFuture(new RuntimeException("Kafka 연결 실패"));
        when(kafkaTemplate.send(anyString(), anyString(), any())).thenReturn(failedFuture);

        UserRegistrationEvent event = new UserRegistrationEvent(
                UUID.randomUUID().toString(),
                "fail@example.com",
                "테스터",
                Instant.now()
        );

        assertDoesNotThrow(() -> producer.send(event));
    }
}
