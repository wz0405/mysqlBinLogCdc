package com.sync.demo.biz.sqsSubscriber;

import com.sync.demo.SpringContextHolder;
import com.sync.demo.biz.sqsSubscriber.factory.SqsInterface;
import com.sync.demo.biz.sqsSubscriber.service.SqsSubscribeJob;
import com.sync.demo.biz.sqsSubscriber.service.meta.SqsDto;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import javax.sql.DataSource;
@Slf4j
@Service
public class SqsMessageHandler {

    @Autowired
    private ApplicationContext ctx;

    public void messageListener(Message<String> message, String queue) {
        String messageId = (String) message.getHeaders().getOrDefault("messageId", "unknown");

        log.info("[SQS-RECEIVE] queue={}, messageId={}, body={}", queue, messageId, message.getPayload());

        SqsDto sqsDto = new SqsDto();
        sqsDto.setSqsString(message.getPayload());

        try {
            SqsInterface sqsInterface = ctx.getBean(SqsSubscribeJob.class);

            if (sqsInterface.beforProcess(sqsDto)) {
                sqsInterface.doProcess(sqsDto);
                log.info("[SQS-SUCCESS] queue={}, messageId={}", queue, messageId);
            } else {
                log.warn("[SQS-REJECTED] queue={}, messageId={}", queue, messageId);
            }

        } catch (Exception e) {
            log.error("[SQS-ERROR] queue={}, messageId={}, error={}", queue, messageId, e.getMessage(), e);
        }
    }
}

