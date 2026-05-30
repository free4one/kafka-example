package com.example.kafkaexample.dto;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record UserRegistrationRequest(
        @NotBlank @Email String email,
        @NotBlank @Size(min = 2, max = 100) String username
) {}
