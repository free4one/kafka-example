package com.example.kafkaexample.consumer;

import com.example.kafkaexample.dto.UserRegistrationEvent;
import com.example.kafkaexample.exception.DuplicateEmailException;
import com.example.kafkaexample.service.UserRegistrationService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;

import java.time.Instant;
import java.util.UUID;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class UserRegistrationConsumerTest {

    @Mock
    private UserRegistrationService userRegistrationService;

    @Mock
    private Acknowledgment ack;

    private UserRegistrationConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer = new UserRegistrationConsumer(userRegistrationService);
    }

    private UserRegistrationEvent makeEvent(String email) {
        return new UserRegistrationEvent(UUID.randomUUID().toString(), email, "홍길동", Instant.now());
    }

    @Test
    @DisplayName("정상 이메일 수신 시 서비스 호출 후 ACK")
    void consume_newEmail_registersAndAcks() {
        UserRegistrationEvent event = makeEvent("new@example.com");

        consumer.consume(event, ack);

        verify(userRegistrationService).register(event);
        verify(ack).acknowledge();
    }

    @Test
    @DisplayName("중복 이메일 수신 시 서비스가 DuplicateEmailException 던지면 ACK만 전송")
    void consume_duplicateEmail_skipsServiceAndAcks() {
        UserRegistrationEvent event = makeEvent("dup@example.com");
        doThrow(new DuplicateEmailException("dup@example.com"))
                .when(userRegistrationService).register(event);

        consumer.consume(event, ack);

        verify(ack).acknowledge();
    }

    @Test
    @DisplayName("서비스에서 예외 발생 시 ACK 미전송")
    void consume_serviceThrows_doesNotAck() {
        UserRegistrationEvent event = makeEvent("err@example.com");
        doThrow(new RuntimeException("DB 오류")).when(userRegistrationService).register(event);

        try {
            consumer.consume(event, ack);
        } catch (RuntimeException ignored) {
        }

        verify(ack, never()).acknowledge();
    }
}
