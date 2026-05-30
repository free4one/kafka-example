package com.example.kafkaexample.exception;

public class DuplicateEmailException extends RuntimeException {

    public DuplicateEmailException(String email) {
        super("이미 등록된 이메일: " + email);
    }
}
