package com.example.kafkaexample.producer;

import com.example.kafkaexample.dto.UserRegistrationEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class UserRegistrationProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${kafka.topic.user-registration}")
    private String topic;

    public void send(UserRegistrationEvent event) {
        log.info("이벤트 발행 topic={} email={}", topic, event.email());
        kafkaTemplate.send(topic, event.email(), event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("이벤트 발행 실패 topic={} email={}", topic, event.email(), ex);
                    } else {
                        log.debug("이벤트 발행 성공 offset={} email={}",
                                result.getRecordMetadata().offset(), event.email());
                    }
                });
    }
}
