package com.example.kafkaexample.integration;

import com.example.kafkaexample.dto.UserRegistrationRequest;
import com.example.kafkaexample.repository.UserRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers(disabledWithoutDocker = true)
@EmbeddedKafka(
        partitions = 3,
        topics = {"user-registration-topic", "user-registration-dlt"}
)
class UserRegistrationIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:17-alpine");

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("kafka.topic.user-registration", () -> "user-registration-topic");
        registry.add("kafka.topic.user-registration-dlt", () -> "user-registration-dlt");
        registry.add("kafka.consumer.group-id", () -> "integration-test-group");
    }

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private UserRepository userRepository;

    @BeforeEach
    void cleanUp() {
        userRepository.deleteAll();
    }

    @Test
    @DisplayName("정상 등록 흐름: HTTP → Kafka → Consumer → DB 저장")
    void register_fullFlow_savesToDatabase() {
        var request = new UserRegistrationRequest("flow@example.com", "흐름테스터");

        ResponseEntity<Void> response =
                restTemplate.postForEntity("/api/users/register", request, Void.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> userRepository.existsByEmail("flow@example.com"));
    }

    @Test
    @DisplayName("중복 이메일 2회 요청 시 DB에 1건만 저장")
    void register_duplicateEmail_savesOnlyOnce() {
        var request = new UserRegistrationRequest("dup@example.com", "중복테스터");

        restTemplate.postForEntity("/api/users/register", request, Void.class);
        restTemplate.postForEntity("/api/users/register", request, Void.class);

        await().atMost(10, TimeUnit.SECONDS)
                .until(() -> userRepository.existsByEmail("dup@example.com"));

        long count = userRepository.findAll().stream()
                .filter(u -> "dup@example.com".equals(u.getEmail()))
                .count();
        assertThat(count).isEqualTo(1);
    }

    @Test
    @DisplayName("유효성 검증 실패 시 400 응답 반환")
    void register_invalidRequest_returns400() {
        var request = new UserRegistrationRequest("", "a");

        ResponseEntity<Void> response =
                restTemplate.postForEntity("/api/users/register", request, Void.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }
}
