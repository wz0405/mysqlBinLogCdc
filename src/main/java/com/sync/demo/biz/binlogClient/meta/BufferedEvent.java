package com.sync.demo.biz.binlogClient.meta;

import java.util.Map;

public class BufferedEvent {
    public Map<String, Object> row;
    public String eventType;
    public BufferedEvent(Map<String, Object> row, String eventType) {
        this.row = row;
        this.eventType = eventType;
    }
}