package com.sync.demo.biz.binlogClient.method;

import com.sync.demo.SpringContextHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
public class SqsPublisher {

    private final SqsAsyncClient sqsAsyncClient;


    public SqsPublisher(SqsAsyncClient sqsAsyncClient) {
        this.sqsAsyncClient = sqsAsyncClient;
    }

    public void sendMessage(String queueName, String messageId, String messageBody) {
        try {
            // Message Attributes 세팅
            Map<String, MessageAttributeValue> attributes = new HashMap<>();
            attributes.put("messageId", MessageAttributeValue.builder()
                    .dataType("String")
                    .stringValue(messageId)
                    .build());

            // Request 빌드
            SendMessageRequest.Builder requestBuilder = SendMessageRequest.builder()
                    .queueUrl("https://sqs.ap-northeast-2.amazonaws.com/testid/" + queueName)
                    .messageBody(messageBody)
                    .messageAttributes(attributes);

            // FIFO 큐 처리
            if (queueName.endsWith(".fifo")) {
                requestBuilder.messageGroupId("testGrp")
                        .messageDeduplicationId(messageId);
            }

            SendMessageRequest sendMessageRequest = requestBuilder.build();

            log.info("SQS 발송 → messageId={}, queueName={}, body={}", messageId, queueName, messageBody);

            // 비동기 발송
            CompletableFuture<SendMessageResponse> futureResponse = sqsAsyncClient.sendMessage(sendMessageRequest);

            // 동기 결과 확인 (join 사용)
            SendMessageResponse response = futureResponse.join();

            if (!response.sdkHttpResponse().isSuccessful()) {
                handleFailure("HTTP 응답 실패, status=" + response.sdkHttpResponse().statusCode());
            }

            log.info("SQS 전송 성공 → MessageId={}", response.messageId());

        } catch (Exception e) {
            handleFailure("예외 발생: " + e.getMessage());
        }
    }

    private void handleFailure(String reason) {
        log.error("SQS 발송 실패: {}", reason);

    }
}