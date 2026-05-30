package com.example.kafkaexample.controller;

import com.example.kafkaexample.dto.UserRegistrationRequest;
import com.example.kafkaexample.producer.UserRegistrationProducer;
import com.example.kafkaexample.service.UserRegistrationService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(UserRegistrationController.class)
class UserRegistrationControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private UserRegistrationService userRegistrationService;

    @MockitoBean
    private UserRegistrationProducer userRegistrationProducer;

    @Test
    @DisplayName("유효한 요청 시 202 Accepted 반환")
    void register_validRequest_returns202() throws Exception {
        mockMvc.perform(post("/api/users/register")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "email": "valid@example.com",
                                  "username": "테스터"
                                }
                                """))
                .andExpect(status().isAccepted());

        verify(userRegistrationProducer).send(any());
    }

    @Test
    @DisplayName("빈 이메일로 요청 시 400 Bad Request")
    void register_emptyEmail_returns400() throws Exception {
        mockMvc.perform(post("/api/users/register")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "email": "",
                                  "username": "테스터"
                                }
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("유효성 검증 실패"));
    }

    @Test
    @DisplayName("잘못된 이메일 형식으로 요청 시 400 Bad Request")
    void register_invalidEmailFormat_returns400() throws Exception {
        mockMvc.perform(post("/api/users/register")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "email": "invalid-email",
                                  "username": "테스터"
                                }
                                """))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("너무 짧은 username으로 요청 시 400 Bad Request")
    void register_shortUsername_returns400() throws Exception {
        mockMvc.perform(post("/api/users/register")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "email": "valid@example.com",
                                  "username": "a"
                                }
                                """))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("null 이메일로 요청 시 400 Bad Request")
    void register_nullEmail_returns400() throws Exception {
        mockMvc.perform(post("/api/users/register")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "username": "테스터"
                                }
                                """))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("null username으로 요청 시 400 Bad Request")
    void register_nullUsername_returns400() throws Exception {
        mockMvc.perform(post("/api/users/register")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "email": "valid@example.com"
                                }
                                """))
                .andExpect(status().isBadRequest());
    }
}
