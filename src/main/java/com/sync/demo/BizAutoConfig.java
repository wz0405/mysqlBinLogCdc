package com.sync.demo;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
public class BizAutoConfig {


    @Configuration
    @ConditionalOnProperty(name = "binlogClient.enabled", havingValue = "true", matchIfMissing = false)
    @ComponentScan("com.sync.demo.biz.binlogClient")
    static class BinlogClientConfig {}


    @Configuration
    @ConditionalOnProperty(name = "sqsSubscriber.enabled", havingValue = "true", matchIfMissing = false)
    @ComponentScan("com.sync.demo.biz.sqsSubscriber")
    static class SqsSubscriberConfig {}
}