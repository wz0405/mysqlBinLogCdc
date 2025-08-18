package com.sync.demo.biz.sqsSubscriber;

import io.awspring.cloud.sqs.annotation.SqsListener;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.messaging.Message;

import java.util.Arrays;
import java.util.List;


@Configuration
@Slf4j
@RequiredArgsConstructor
public class SqsMessageListener {

    private final SqsMessageHandler processor;


    @SqsListener("sampleQueue")
    public void handleBaseInfo(Message<String> message) {
        processIfTargetQueue(message, "sampleQueue");
    }


    private void processIfTargetQueue(Message<String> message, String queueName) {
       processor.messageListener(message, queueName);

    }
}
