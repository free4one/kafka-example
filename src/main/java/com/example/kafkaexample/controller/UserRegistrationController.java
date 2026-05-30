package com.example.kafkaexample.controller;

import com.example.kafkaexample.dto.UserRegistrationEvent;
import com.example.kafkaexample.dto.UserRegistrationRequest;
import com.example.kafkaexample.producer.UserRegistrationProducer;
import com.example.kafkaexample.service.UserRegistrationService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.UUID;

@RestController
@RequestMapping("/api/users")
@Slf4j
@RequiredArgsConstructor
public class UserRegistrationController {

    private final UserRegistrationService userRegistrationService;
    private final UserRegistrationProducer producer;

    @PostMapping("/register")
    public ResponseEntity<Void> register(@Valid @RequestBody UserRegistrationRequest request) {
        log.info("등록 요청 수신 email={}", request.email());
        userRegistrationService.validateEmailAvailable(request.email());

        UserRegistrationEvent event = new UserRegistrationEvent(
                UUID.randomUUID().toString(),
                request.email(),
                request.username(),
                Instant.now()
        );

        producer.send(event);
        return ResponseEntity.accepted().build();
    }
}
