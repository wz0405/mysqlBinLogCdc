package com.sync.demo.biz.sqsSubscriber.service.meta;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class SqsMessage {
    private String eventType;
    private String tableName;
    private String manageType;
    private Object dataSource;

    private List<String> keyList;
    private List<String> valueList;
    private Map<String, Object> pkSourceMap;
    private String targetDB;
    private Map<String, Object> dataSourceMap;
    private String columnName;
}