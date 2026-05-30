package com.example.kafkaexample.service;

import com.example.kafkaexample.dto.UserRegistrationEvent;
import com.example.kafkaexample.entity.User;
import com.example.kafkaexample.entity.UserStatus;
import com.example.kafkaexample.exception.DuplicateEmailException;
import com.example.kafkaexample.repository.UserRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class UserRegistrationServiceTest {

    @Mock
    private UserRepository userRepository;

    private UserRegistrationService service;

    @BeforeEach
    void setUp() {
        service = new UserRegistrationService(userRepository);
    }

    private UserRegistrationEvent makeEvent(String email, String username) {
        return new UserRegistrationEvent(UUID.randomUUID().toString(), email, username, Instant.now());
    }

    @Test
    @DisplayName("신규 이메일로 등록 시 User 저장")
    void register_newEmail_saveUser() {
        UserRegistrationEvent event = makeEvent("new@example.com", "신규사용자");
        when(userRepository.existsByEmail("new@example.com")).thenReturn(false);
        when(userRepository.save(any(User.class))).thenAnswer(invocation -> {
            User user = invocation.getArgument(0);
            return User.builder()
                    .id(1L)
                    .email(user.getEmail())
                    .username(user.getUsername())
                    .status(UserStatus.ACTIVE)
                    .createdAt(Instant.now())
                    .updatedAt(Instant.now())
                    .build();
        });

        service.register(event);

        verify(userRepository).existsByEmail("new@example.com");
        verify(userRepository).save(any(User.class));
    }

    @Test
    @DisplayName("중복 이메일로 등록 시 DuplicateEmailException 던짐")
    void register_duplicateEmail_throwsException() {
        UserRegistrationEvent event = makeEvent("dup@example.com", "사용자");
        when(userRepository.existsByEmail("dup@example.com")).thenReturn(true);

        assertThatThrownBy(() -> service.register(event))
                .isInstanceOf(DuplicateEmailException.class)
                .hasMessageContaining("dup@example.com");

        verify(userRepository).existsByEmail("dup@example.com");
        verify(userRepository, never()).save(any());
    }

    @Test
    @DisplayName("등록된 User 저장 시 ACTIVE 상태로 저장됨")
    void register_savedUser_hasActiveStatus() {
        UserRegistrationEvent event = makeEvent("active@example.com", "활성사용자");
        when(userRepository.existsByEmail("active@example.com")).thenReturn(false);
        User savedUser = User.builder()
                .id(2L)
                .email("active@example.com")
                .username("활성사용자")
                .status(UserStatus.ACTIVE)
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .build();
        when(userRepository.save(any(User.class))).thenReturn(savedUser);

        service.register(event);

        verify(userRepository).save(argThat(user ->
                user.getEmail().equals("active@example.com") &&
                user.getUsername().equals("활성사용자")
        ));
    }
}
