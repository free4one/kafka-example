package com.example.kafkaexample.service;

import com.example.kafkaexample.dto.UserRegistrationEvent;
import com.example.kafkaexample.entity.User;
import com.example.kafkaexample.entity.UserStatus;
import com.example.kafkaexample.exception.DuplicateEmailException;
import com.example.kafkaexample.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Slf4j
@RequiredArgsConstructor
public class UserRegistrationService {

    private final UserRepository userRepository;

    @Transactional(readOnly = true)
    public void validateEmailAvailable(String email) {
        if (userRepository.existsByEmail(email)) {
            throw new DuplicateEmailException(email);
        }
    }

    @Transactional
    public void register(UserRegistrationEvent event) {
        if (userRepository.existsByEmail(event.email())) {
            throw new DuplicateEmailException(event.email());
        }

        User user = User.builder()
                .email(event.email())
                .username(event.username())
                .status(UserStatus.PENDING)
                .build();

        userRepository.save(user);
        log.info("사용자 등록 완료 email={}", event.email());
    }
}
