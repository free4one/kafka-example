package com.example.kafkaexample.dto;

import java.time.Instant;

public record UserRegistrationEvent(
        String eventId,
        String email,
        String username,
        Instant requestedAt
) {}
