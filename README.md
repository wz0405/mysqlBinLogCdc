---

```markdown
# MySQL Binlog 기반 CDC (Change Data Capture) 데모

본 프로젝트는 **MySQL Binlog 이벤트를 읽어 데이터 변화를 감지**하고,  
이를 가공한 뒤 **SQS 또는 HTTP API로 전달**하고,  
수신부에서 **SQS 메시지를 받아 DB에 DML 적용**하는 CDC 데모 애플리케이션입니다.  

📌 목적:  
- DB의 `INSERT / UPDATE / DELETE` 이벤트를 **실시간 스트리밍**  
- 메시지를 **SQS 발행**하거나 **HTTP API 호출**로 전달  
- 수신부에서 메시지를 **DB DML(INSERT/UPDATE/DELETE)** 로 반영  
- Buffering + Batch 전송(HTTP) & 즉시 전송(SQS) 지원  

---

## 📂 프로젝트 구조

### 발신부 (binlogClient)
```

com.sync.demo.biz.binlogClient
├── mapper
│   └── BinlogClientMapper.java   # (옵션) 메타데이터 조회
├── meta
│   ├── BufferedEvent.java        # 이벤트 버퍼 객체
│   └── ColumnMeta.java           # 컬럼 메타 정보
└── method
├── BinlogClientListener.java # Binlog 이벤트 리스너 (핵심)
├── BinlogClientMethod.java   # 이벤트 파싱 (Insert/Update/Delete)
├── BinlogHttpSender.java     # HTTP 전송 모듈
└── SqsPublisher.java         # SQS 전송 모듈

```

### 수신부 (sqsSubscriber)
```

com.sync.demo.biz.sqsSubscriber
├── factory
│   ├── DBJobInterface.java       # DB 처리 인터페이스
│   ├── SqsInterface.java         # SQS 처리 인터페이스
│   └── TableJobFactory.java      # 테이블별 Job Factory
├── mapper
│   └── SqsCommonMapper.java      # 동적 DML 실행 (MyBatis)
├── meta
│   └── ColumInfo.java            # 컬럼 메타정보
└── service
├── SqsMessageListener.java   # SQS Listener (수신 시작점)
├── SqsMessageHandler.java    # 메시지 파싱 및 예외 처리
├── SqsMethod.java            # 공통 DML 처리
├── SqsSubMethod.java         # 테이블별 DML 처리
├── SqsSubscribeJob.java      # Job 단위 실행
└── meta
├── SqsDto.java
└── SqsMessage.java       # 메시지 DTO

````

---

## ⚙️ 동작 방식

### 발신부
1. **BinlogClientListener**
   - `BinaryLogClient` 로 MySQL Binlog 이벤트 구독
   - `INSERT / UPDATE / DELETE` 이벤트 감지
   - `MODE` 옵션에 따라 동작 분기:
     - `SQS` → 감지 즉시 SQS 발송
     - `HTTP` → 버퍼에 적재 후 일정 건수/주기로 Batch 전송

2. **SqsPublisher**
   - AWS SQS에 메시지 발송
   - FIFO 큐 지원 (`messageGroupId`, `messageDeduplicationId`)

3. **BinlogHttpSender**
   - 버퍼링된 이벤트를 외부 API 엔드포인트로 POST 전송  

---

### 수신부
1. **SqsMessageListener**
   - 지정된 Queue로부터 메시지 수신
   - JSON 문자열 그대로 `SqsMessageHandler`로 전달  

2. **SqsMessageHandler**
   - 메시지 파싱 (`SqsMessage`, `SqsDto`)
   - 트랜잭션 단위로 Job 실행
   - 실패 시 `SqsFailureHandler` 로 로깅/보관

3. **SqsSubMethod / SqsMethod**
   - 공통 DML 로직 (`INSERT`, `UPDATE`, `DELETE`)
   - 테이블/컬럼 메타정보 기반 SQL 빌드

4. **SqsCommonMapper (MyBatis)**
   - 동적 SQL 실행 (`<foreach>`, `<choose>` 활용)
   - Primary Key 기반 Update/Delete 지원
   - 메타 테이블(`information_schema`) 조회 가능  

---

## 🛠️ 주요 코드

### Binlog 이벤트 → SQS 발송
```java
sqsPublisher.sendMessage("binlog-queue.fifo", messageId, row.toString());
````

### SQS 수신 → Handler 처리

```java
@SqsListener("binlog-queue.fifo")
public void receiveMessage(String message) {
    log.info("SQS 메시지 수신: {}", message);
    sqsMessageHandler.handleMessage(message);
}
```

### Mapper (동적 Update 예시)

```xml
<update id="updateTables" parameterType="com.sync.demo.biz.sqsSubscriber.service.meta.SqsMessage">
    UPDATE ${targetDB}.${tableName}
    <set>
        <foreach collection="dataSourceMap" item="value" index="key" separator=",">
            ${key} = #{value}
        </foreach>
    </set>
    WHERE
    <foreach collection="pkSourceMap" item="value" index="key" separator="AND">
        ${key} = #{value}
    </foreach>
</update>
```

---

## 🚀 실행 방법

1. **MySQL Binlog 활성화**

```sql
SHOW VARIABLES LIKE 'log_bin';
SHOW VARIABLES LIKE 'binlog_format'; -- ROW 모드 필요
```

2. **환경설정**

* DB 접속정보 (`host`, `user`, `password`)
* AWS SQS 권한 (`~/.aws/credentials` or IAM Role)
* 외부 API URL (`BinlogHttpSender`)


3. **결과 확인**

* DB `INSERT/UPDATE/DELETE` 발생 시:

  * `SQS` 모드 → Queue에 메시지 즉시 발행
  * `HTTP` 모드 → 주기적으로 외부 API 호출
* 수신부 실행 시:

  * Queue 메시지를 파싱하여 DB에 `DML 반영`

---

## 📌 특징

* Binlog 기반 **비동기 CDC**
* **SQS 즉시 전송** vs **HTTP Batch 전송** 옵션 지원
* **Buffering & Flush** 전략으로 데이터 유실 방지
* **FIFO Queue** 지원 (순서 보장)
* 수신부에서 **동적 SQL 생성**으로 테이블 추가 시 확장성 높음

---

## 📝 예시 로그

```
[BINLOG] INSERT event detected: table=users, rowCount=1
SQS 발송 → messageId=123e4567..., queueName=binlog-queue.fifo, body={id=1, name="Alice"}
SQS 전송 성공 → MessageId=abc-123-def

SQS 메시지 수신: { "table":"users", "op":"INSERT", "data":{"id":1, "name":"Alice"} }
[DB-INSERT] table=users, data={id=1, name=Alice}
```

```

---
