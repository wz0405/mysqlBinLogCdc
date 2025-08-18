package com.sync.demo.biz.binlogClient.method;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BinlogHttpSender extends Thread {

    private final String payload;
    private final String targetUrl;

    public BinlogHttpSender(String payload, String targetUrl) {
        this.payload = payload;
        this.targetUrl = targetUrl;
    }

    @Override
    public void run() {
        try {
            // 실제에선 RestTemplate/WebClient/HttpUtils 사용
            log.info("POST {} -> {}", targetUrl, payload);
        } catch (Exception e) {
            log.error("Failed to send binlog event", e);
        }
    }
}
