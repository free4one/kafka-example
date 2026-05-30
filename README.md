# kafka-example

Kafka Consumer를 이용한 비동기 사용자 등록 처리 예제 프로젝트.

REST API로 등록 요청을 받아 Kafka에 이벤트를 발행하고, Consumer가 이를 소비해 PostgreSQL에 저장하는 전체 흐름을 구현합니다.

```
POST /api/users/register
        │
        ▼
UserRegistrationController  (유효성 검증 + 중복 이메일 동기 체크)
        │
        ▼
UserRegistrationProducer  →  user-registration-topic
                                      │
                                      ▼ (비동기)
                             UserRegistrationConsumer
                                      │
                                      ▼
                             UserRegistrationService  →  PostgreSQL (users)
                                      │
                              실패 시 재시도 3회
                                      │
                                      ▼
                             user-registration-dlt  (Dead Letter Topic)
```

---

## 기술 스택

| 구성요소 | 버전 |
|---|---|
| Java | 21 (LTS) |
| Spring Boot | 3.4.x |
| Spring Kafka | 3.3.x |
| PostgreSQL | 17.x |
| Gradle | 8.x (Kotlin DSL) |
| Flyway | 10.x |
| Testcontainers | 1.20.x |

---

## 사전 요구사항

- JDK 21+
- Docker Desktop

---

## 빠른 시작

```bash
# 1. 인프라 실행 (Kafka + PostgreSQL + Kafka UI)
docker compose up -d

# 2. 빌드
./gradlew build

# 3. 실행
./gradlew bootRun

# 4. 사용자 등록 요청
curl -X POST http://localhost:8080/api/users/register \
  -H "Content-Type: application/json" \
  -d '{"email":"user@example.com","username":"홍길동"}'

# 5. 전체 테스트
./gradlew test
```

---

## API 명세

### POST /api/users/register

사용자 등록 이벤트를 Kafka에 발행합니다. 처리는 비동기로 진행됩니다.

**요청 본문**

```json
{
  "email": "user@example.com",
  "username": "홍길동"
}
```

| 필드 | 타입 | 제약 |
|---|---|---|
| `email` | string | 필수, 이메일 형식 |
| `username` | string | 필수, 2~100자 |

**응답 코드**

| 상태 코드 | 설명 |
|---|---|
| `202 Accepted` | 등록 이벤트 발행 성공 (비동기 처리 진행) |
| `400 Bad Request` | 유효성 검증 실패 (이메일 형식 오류, 길이 초과 등) |
| `409 Conflict` | 이미 등록된 이메일 |
| `500 Internal Server Error` | 서버 내부 오류 |

---

## 프로젝트 구조

```
src/main/java/com/example/kafkaexample/
├── KafkaExampleApplication.java
├── config/
│   ├── KafkaConsumerConfig.java    # Consumer + DLQ ErrorHandler 설정
│   ├── KafkaProducerConfig.java    # Producer JsonSerializer 설정
│   └── KafkaTopicConfig.java       # Topic 빈 정의
├── controller/
│   ├── UserRegistrationController.java
│   └── GlobalExceptionHandler.java
├── dto/
│   ├── UserRegistrationRequest.java    # REST 요청 (Bean Validation)
│   └── UserRegistrationEvent.java      # Kafka 메시지 레코드
├── entity/
│   ├── User.java
│   └── UserStatus.java                 # PENDING / ACTIVE
├── exception/
│   └── DuplicateEmailException.java
├── repository/
│   └── UserRepository.java
├── service/
│   └── UserRegistrationService.java    # 비즈니스 로직, @Transactional
├── producer/
│   └── UserRegistrationProducer.java
└── consumer/
    └── UserRegistrationConsumer.java
```

---

## Kafka 설계

### Topic 구성

| Topic | 용도 |
|---|---|
| `user-registration-topic` | 사용자 등록 이벤트 |
| `user-registration-dlt` | 처리 실패 메시지 (Dead Letter) |

- 파티션: 3개
- 파티션 키: `email` (동일 이메일 → 동일 파티션 → 처리 순서 보장)
- Consumer Group ID: `user-registration-group`

### 에러 핸들링

- `DefaultErrorHandler` + `DeadLetterPublishingRecoverer` 조합
- 재시도: 최대 3회, 지수 백오프 (1s → 2s → 4s)
- 3회 모두 실패 시 `user-registration-dlt`로 이동
- DLT 메시지에는 원본 헤더 + 예외 정보 자동 포함

### 메시지 포맷 (UserRegistrationEvent)

```json
{
  "eventId": "550e8400-e29b-41d4-a716-446655440000",
  "email": "user@example.com",
  "username": "홍길동",
  "requestedAt": "2026-05-30T10:00:00Z"
}
```

### 멱등성 처리 (이중 보호)

1. **Controller 레벨**: `validateEmailAvailable()` — 동기 중복 체크, 중복 시 즉시 409 반환
2. **Consumer 레벨**: `existsByEmail()` — 비동기 처리 중 경쟁 조건 방어
3. **DB 레벨**: `email UNIQUE` 제약 — 최종 안전망

---

## 데이터베이스

마이그레이션 파일: `src/main/resources/db/migration/V1__create_users_table.sql`

### users 테이블

| 컬럼 | 타입 | 비고 |
|---|---|---|
| `id` | BIGSERIAL PK | |
| `email` | VARCHAR(255) UNIQUE NOT NULL | |
| `username` | VARCHAR(100) NOT NULL | |
| `status` | VARCHAR(20) NOT NULL | `PENDING` / `ACTIVE` |
| `created_at` | TIMESTAMPTZ | |
| `updated_at` | TIMESTAMPTZ | `@PreUpdate` 자동 갱신 |

---

## 테스트

```bash
# 전체 테스트 실행
./gradlew test

# 커버리지 리포트 생성 (build/reports/jacoco/)
./gradlew test jacocoTestReport
```

### 테스트 전략

| 테스트 | 위치 | 방법 |
|---|---|---|
| Consumer 단위 | `consumer/UserRegistrationConsumerTest` | Mockito `@MockBean` |
| Producer 단위 | `producer/UserRegistrationProducerTest` | `@EmbeddedKafka` |
| 전체 통합 | `integration/UserRegistrationIntegrationTest` | Testcontainers |

- 최소 커버리지: **80%**
- 비동기 검증: `Awaitility` 사용 (`Thread.sleep` 금지)

---

## 환경 변수

| 환경 변수 | 기본값 | 설명 |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka 브로커 주소 |
| `KAFKA_CONSUMER_GROUP_ID` | `user-registration-group` | Consumer 그룹 ID |
| `POSTGRES_HOST` | `localhost` | PostgreSQL 호스트 |
| `POSTGRES_PORT` | `5432` | PostgreSQL 포트 |
| `POSTGRES_DB` | `kafkaexample` | 데이터베이스 이름 |
| `POSTGRES_USER` | `postgres` | 데이터베이스 사용자 |
| `POSTGRES_PASSWORD` | `postgres` | 데이터베이스 비밀번호 |

---

## 개발 도구

### Kafka UI

Kafka 토픽, 메시지, Consumer 그룹을 웹 UI로 모니터링합니다.

```
http://localhost:8989
```

`docker compose up -d` 실행 후 자동으로 접근 가능합니다.
