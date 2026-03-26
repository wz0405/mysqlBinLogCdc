# MySQL Binlog 기반 CDC (Change Data Capture) 데모

본 프로젝트는 Debezium 없이 Java 레이어에서 직접 MySQL Binlog를 파싱하여 CDC를 구현한 데모입니다.  
인프라 의존 없이 애플리케이션 레벨에서 CDC를 제어해야 하는 경우, 또는 Debezium 같은 도구가 내부적으로 어떻게 동작하는지 이해하기 위한 참고 구현체로 작성했습니다.

> Debezium + Kafka Connect 기반의 인프라 레벨 CDC 구현은 [debezium-cdc-pipeline](https://github.com/wz0405/debeziumCdcPipeline) 레포를 참조하십시오.

---

## 목적

- DB의 `INSERT / UPDATE / DELETE` 이벤트를 실시간 스트리밍
- 이벤트를 SQS 즉시 발행 또는 HTTP Batch 전송으로 전달
- 수신부에서 메시지를 받아 DB에 DML 반영
- 발신부 / 수신부를 `application.properties` 플래그 하나로 독립 ON/OFF

---

## 기술 스택

- Java 17 / Spring Boot 3.2.3
- mysql-binlog-connector-java 0.21.0 (`BinaryLogClient`)
- AWS SQS — `spring-cloud-aws-starter-sqs 3.0.1` / `SqsAsyncClient`
- MyBatis 3.0.3 / MariaDB
- Gson 2.9.1 / Jasypt (설정값 암호화)

---

## 프로젝트 구조

```plaintext
com.sync.demo
├── BizAutoConfig               # @ConditionalOnProperty 기반 모듈 ON/OFF
├── ApiConfig                   # MyBatis SqlSessionFactory, @MapperScan
├── SpringContextHolder         # ApplicationContext 정적 접근
│
├── biz.binlogClient            # 발신부
│   ├── mapper
│   │   └── BinlogClientMapper      # INFORMATION_SCHEMA 컬럼 메타 조회
│   ├── meta
│   │   ├── BufferedEvent           # 이벤트 버퍼 객체 (row + eventType)
│   │   └── ColumnMeta              # 컬럼 메타정보 (PK, 타입, nullable 등)
│   └── method
│       ├── BinlogClientListener    # Binlog 이벤트 구독, 버퍼링, flush (핵심)
│       ├── BinlogClientMethod      # INSERT/UPDATE/DELETE 행 추출 공통 로직
│       ├── BinlogHttpSender        # HTTP Batch 전송 (Thread 기반)
│       └── SqsPublisher            # SQS 비동기 발송 (FIFO 지원)
│
└── biz.sqsSubscriber           # 수신부
    ├── factory
    │   ├── SqsInterface            # beforProcess / doProcess / afterProcess
    │   ├── DBJobInterface          # 테이블별 DML 인터페이스
    │   └── TableJobFactory         # 테이블명 기반 Bean 동적 조회
    ├── mapper
    │   └── SqsCommonMapper         # 동적 DML 4종 (INSERT/UPDATE/DELETE/UPSERT)
    ├── meta
    │   └── ColumInfo               # INFORMATION_SCHEMA 조회 결과 매핑
    ├── service
    │   ├── SqsSubscribeJob         # SqsInterface 구현체 (진입점)
    │   └── meta
    │       ├── SqsMessage          # 메시지 단위 DTO (eventType, tableName, pkMap, dataMap)
    │       └── SqsDto              # 메시지 배치 DTO
    ├── SqsMessageListener      # @SqsListener — 큐별 수신 시작점
    ├── SqsMessageHandler       # 메시지 파싱, Job 실행, 예외 처리
    ├── SqsMethod               # 메시지 검증, eventType 분기
    └── SqsSubMethod            # INSERT / UPDATE / DELETE / UPSERT 실행
```

---

## 핵심 설계 포인트

### 모듈 독립 ON/OFF — BizAutoConfig

`@ConditionalOnProperty`로 발신부(`binlogClient`)와 수신부(`sqsSubscriber`)를 완전히 분리합니다.  
같은 코드베이스에서 역할별로 독립 배포하거나, 한쪽만 활성화해 테스트할 수 있습니다.

```java
@Configuration
public class BizAutoConfig {

    @Configuration
    @ConditionalOnProperty(name = "binlogClient.enabled", havingValue = "true")
    @ComponentScan("com.sync.demo.biz.binlogClient")
    static class BinlogClientConfig {}

    @Configuration
    @ConditionalOnProperty(name = "sqsSubscriber.enabled", havingValue = "true")
    @ComponentScan("com.sync.demo.biz.sqsSubscriber")
    static class SqsSubscriberConfig {}
}
```

```properties
# application.properties
binlogClient.enabled=true    # 발신부만 활성화
sqsSubscriber.enabled=false
```

---

### 발신부 — BinlogClientListener

`BinaryLogClient`로 Binlog 이벤트를 구독하고, `TableMapEventData` 캐시로 테이블 정보를 유지합니다.  
`MODE` 설정에 따라 SQS 즉시 발송과 HTTP 버퍼링 발송으로 분기합니다.  
버퍼는 `ConcurrentHashMap`으로 관리하며, 건수 임계값(`BUFFER_SIZE_THRESHOLD=10`) 도달 또는  
`@Scheduled(fixedRate=1000)` 주기 flush 중 먼저 도달하는 조건으로 전송합니다.

```java
@PostConstruct
public void start() {
    new Thread(() -> {
        try { runBinlogClient(); }
        catch (Exception e) { log.error("Binlog client error", e); }
    }, "Binlog-Client-Thread").start();
}

private void runBinlogClient() throws Exception {
    BinaryLogClient client = new BinaryLogClient("localhost", 3306, "user", "password");
    client.setServerId(new Random().nextInt(100000)); // 복제 슬레이브 ID 충돌 방지

    client.registerEventListener(event -> {
        EventData data = event.getData();

        if (data instanceof TableMapEventData tableMap) {
            tableMapCache.put(tableMap.getTableId(), tableMap); // 테이블 메타 캐싱
            return;
        }
        if (data instanceof WriteRowsEventData write) {
            handleRowEvent(write.getTableId(),
                extractInsertRows(write, tableMapCache, columnMetaCache), "INSERT");
        } else if (data instanceof UpdateRowsEventData update) {
            handleRowEvent(update.getTableId(),
                extractUpdatedRows(update, tableMapCache, columnMetaCache), "UPDATE");
        } else if (data instanceof DeleteRowsEventData delete) {
            handleRowEvent(delete.getTableId(),
                extractDeletedRows(delete, tableMapCache, columnMetaCache), "DELETE");
        }
    });

    client.connect();
}

private void handleRowEvent(long tableId, List<Map<String, Object>> rows, String eventType) {
    TableMapEventData tableMap = tableMapCache.get(tableId);
    if (tableMap == null || rows.isEmpty()) return;

    if ("SQS".equalsIgnoreCase(MODE)) {
        // 즉시 발송
        rows.forEach(row ->
            sqsPublisher.sendMessage("binlog-queue.fifo", UUID.randomUUID().toString(), row.toString())
        );
    }
    if ("HTTP".equalsIgnoreCase(MODE)) {
        // 버퍼링 후 flush
        bufferAndMaybeFlush(tableMap, rows, eventType);
    }
}

/** 주기적으로 HTTP 잔여 버퍼 flush */
@Scheduled(fixedRate = 1000)
public void flushRemainingData() {
    if (!"HTTP".equalsIgnoreCase(MODE)) return;
    eventBuffer.forEach((table, buffer) ->
        eventBuffer.compute(table, (key, buf) -> {
            if (buf == null || buf.isEmpty()) return buf;
            // ... flush 후 버퍼 초기화
            return new ArrayList<>();
        })
    );
}
```

---

### 발신부 — SqsPublisher

`SqsAsyncClient`로 비동기 발송 후 `CompletableFuture.join()`으로 결과를 확인합니다.  
큐명이 `.fifo`로 끝나면 `messageGroupId`, `messageDeduplicationId`를 자동으로 설정합니다.

```java
public void sendMessage(String queueName, String messageId, String messageBody) {
    SendMessageRequest.Builder builder = SendMessageRequest.builder()
            .queueUrl("https://sqs.ap-northeast-2.amazonaws.com/account-id/" + queueName)
            .messageBody(messageBody)
            .messageAttributes(Map.of(
                "messageId", MessageAttributeValue.builder()
                    .dataType("String").stringValue(messageId).build()
            ));

    // FIFO 큐 자동 감지
    if (queueName.endsWith(".fifo")) {
        builder.messageGroupId("defaultGroup")
               .messageDeduplicationId(messageId);
    }

    SendMessageResponse response = sqsAsyncClient.sendMessage(builder.build()).join();

    if (!response.sdkHttpResponse().isSuccessful()) {
        log.error("SQS 발송 실패: status={}", response.sdkHttpResponse().statusCode());
    }
}
```

---

### 수신부 처리 흐름

```
@SqsListener("sampleQueue")
        │
        ▼
SqsMessageHandler.messageListener()
  - MDC logId 설정
  - SqsSubscribeJob Bean 조회
        │
        ▼
SqsSubscribeJob (SqsInterface 구현체)
  ├── beforProcess() → SqsMethod.checkMsg()
  │     JSON 파싱 → List<SqsMessage> 변환, 형식 검증
  └── doProcess()   → SqsMethod.reciveDBJob()
        │
        ▼
SqsMethod.doDBJob() — eventType 분기
  ├── INSERT → SqsSubMethod.normalInsert()
  ├── UPDATE → SqsSubMethod.normalUpdate()
  ├── DELETE → SqsSubMethod.normalDelete()
  └── (확장) UPSERT → SqsSubMethod.normalUpsert()
        │
        ▼
SqsCommonMapper (MyBatis 동적 SQL)
  - targetDB, tableName을 런타임에 주입
  - dataSourceMap / pkSourceMap 기반 DML 실행
```

---

### 수신부 — 동적 DML (SqsCommonMapper.xml)

테이블명과 컬럼을 런타임에 주입하는 동적 SQL로, 테이블 추가 시 Mapper 수정 없이 확장됩니다.  
`INFORMATION_SCHEMA`로 컬럼 메타와 PK 정보를 조회하여 DML을 자동 구성합니다.

```xml
<!-- INSERT: dataSourceMap의 키/값을 동적으로 바인딩 -->
<insert id="insertTables" parameterType="...SqsMessage">
    INSERT INTO ${targetDB}.${tableName} (
        <foreach collection="dataSourceMap" item="value" index="index" separator=",">
            ${index}
        </foreach>
    ) VALUES (
        <foreach collection="dataSourceMap" item="value" index="index" separator=",">
            <choose>
                <when test="value != null">#{value}</when>
                <otherwise>null</otherwise>
            </choose>
        </foreach>
    )
</insert>

<!-- UPDATE: PK 조건 동적 바인딩 -->
<update id="updateTables" parameterType="...SqsMessage">
    UPDATE ${targetDB}.${tableName}
    SET
    <foreach collection="dataSourceMap" item="value" index="key" separator=",">
        <choose>
            <when test="value != null">${key} = #{value}</when>
            <otherwise>${key} = null</otherwise>
        </choose>
    </foreach>
    WHERE
    <foreach collection="pkSourceMap" item="value" index="key" separator="and">
        ${key} = #{value}
    </foreach>
</update>

<!-- UPSERT: isDupCheck → INSERT or UPDATE 분기 -->
<select id="isDupCheck" parameterType="...SqsMessage" resultType="Integer">
    SELECT COUNT(*) FROM ${targetDB}.${tableName}
    WHERE
    <foreach collection="pkSourceMap" item="value" index="key" separator="and">
        ${key} = #{value}
    </foreach>
</select>
```

---

### 수신부 — TableJobFactory (테이블별 확장 포인트)

테이블명으로 `DBJobInterface` Bean을 동적으로 조회합니다.  
공통 DML 로직 외 특정 테이블에 별도 처리가 필요한 경우 `@Service("TABLE_NAME")`으로 구현체를 등록합니다.

```java
@Service
public class TableJobFactory {
    public DBJobInterface getBean(ApplicationContext appCtx, SqsMessage sqsMessage) throws Exception {
        return (DBJobInterface) appCtx.getBean(sqsMessage.getTableName());
    }
}

// 테이블별 커스텀 처리 등록 예시
@Service("SPECIAL_TABLE")
public class SpecialTableJob implements DBJobInterface {
    @Override
    public void insert(SqsMessage sqsMessage) throws Exception { /* 테이블 전용 로직 */ }
    // ...
}
```

---

## 실행 방법

**MySQL Binlog 활성화 확인**

```sql
SHOW VARIABLES LIKE 'log_bin';
SHOW VARIABLES LIKE 'binlog_format'; -- ROW 모드 필요
```

**application.properties 설정**

```properties
# 모듈 활성화 (독립 제어)
binlogClient.enabled=true
sqsSubscriber.enabled=true

# DB
spring.datasource.url=jdbc:mysql://localhost:3306/testDB
spring.datasource.hikari.username=user
spring.datasource.hikari.password=password

# SQS 수신 타겟 DB
subscribe.target.db=targetDB
```

**AWS 자격증명**

```bash
# ~/.aws/credentials 또는 IAM Role
aws_access_key_id=...
aws_secret_access_key=...
```

**실행**

```bash
./gradlew bootRun
```

---

## 동작 로그 예시

```plaintext
# 발신부
[BINLOG] INSERT event detected: table=users, rowCount=1
SQS 발송 → messageId=123e4567..., queueName=binlog-queue.fifo, body={id=1, name=Alice}
SQS 전송 성공 → MessageId=abc-123-def

[BINLOG] Scheduled flush: table=orders, bufferedSize=7
Sending 7 events to external system: table=orders

# 수신부
[SQS-RECEIVE] queue=sampleQueue, messageId=123e4567..., body=[{"eventType":"INSERT",...}]
[DB-INSERT] table=users, data={id=1, name=Alice}
[SQS-SUCCESS] queue=sampleQueue, messageId=123e4567...
```

---

## 정리

- `BinaryLogClient`로 MySQL Binlog를 직접 구독 — Debezium / Kafka 없이 Java 레이어에서 CDC 구현
- `@ConditionalOnProperty`로 발신부 / 수신부를 독립 활성화 — 단일 코드베이스, 역할별 배포 가능
- SQS 즉시 발송 vs HTTP Batch 전송을 `MODE` 상수로 전환 가능
- FIFO 큐 자동 감지로 순서 보장 및 중복 제거 (`messageDeduplicationId`)
- `INFORMATION_SCHEMA` 기반 동적 DML로 테이블 추가 시 Mapper 수정 불필요
- `TableJobFactory`로 테이블별 커스텀 처리를 Bean 등록으로 확장
