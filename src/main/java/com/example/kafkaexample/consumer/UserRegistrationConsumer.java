package com.example.kafkaexample.consumer;

import com.example.kafkaexample.dto.UserRegistrationEvent;
import com.example.kafkaexample.exception.DuplicateEmailException;
import com.example.kafkaexample.service.UserRegistrationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class UserRegistrationConsumer {

    private final UserRegistrationService userRegistrationService;

    @KafkaListener(
            topics = "${kafka.topic.user-registration}",
            groupId = "${kafka.consumer.group-id}"
    )
    public void consume(@Payload UserRegistrationEvent event, Acknowledgment ack) {
        log.info("이벤트 수신 event={}", event);

        try {
            userRegistrationService.register(event);
        } catch (DuplicateEmailException e) {
            log.warn("이미 등록된 이메일 skip email={}", event.email());
            ack.acknowledge();
            return;
        }

        ack.acknowledge();
    }
}
