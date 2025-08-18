package com.sync.demo.biz.binlogClient.method;

import com.github.shyiko.mysql.binlog.event.*;
import com.sync.demo.biz.binlogClient.meta.BufferedEvent;
import com.sync.demo.biz.binlogClient.meta.ColumnMeta;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.*;

@Slf4j
@Component
public class BinlogClientMethod {

    protected final Map<String, Map<String, ColumnMeta>> columnMetaCache = new HashMap<>();

    protected List<Map<String, Object>> extractInsertRows(
            WriteRowsEventData writeData,
            Map<Long, TableMapEventData> tableMapCache,
            Map<String, Map<String, ColumnMeta>> columnMetaCache) {

        TableMapEventData tableMap = tableMapCache.get(writeData.getTableId());
        if (tableMap == null) return List.of();

        List<Map<String, Object>> result = new ArrayList<>();
        for (Serializable[] row : writeData.getRows()) {
            Map<String, Object> rowMap = new LinkedHashMap<>();
            for (int i = 0; i < row.length; i++) {
                rowMap.put("col" + i, row[i]); // 단순 예시
            }
            result.add(rowMap);
        }
        return result;
    }

    protected List<Map<String, Object>> extractUpdatedRows(
            UpdateRowsEventData updateData,
            Map<Long, TableMapEventData> tableMapCache,
            Map<String, Map<String, ColumnMeta>> columnMetaCache) {

        TableMapEventData tableMap = tableMapCache.get(updateData.getTableId());
        if (tableMap == null) return List.of();

        List<Map<String, Object>> result = new ArrayList<>();

        for (Map.Entry<Serializable[], Serializable[]> row : updateData.getRows()) {
            Serializable[] before = row.getKey();
            Serializable[] after = row.getValue();

            Map<String, Object> rowMap = new LinkedHashMap<>();
            for (int i = 0; i < after.length; i++) {
                rowMap.put("col" + i + "_before", before[i]);
                rowMap.put("col" + i + "_after", after[i]);
            }
            result.add(rowMap);
        }

        return result;
    }
    protected List<Map<String, Object>> extractDeletedRows(
            DeleteRowsEventData deleteData,
            Map<Long, TableMapEventData> tableMapCache,
            Map<String, Map<String, ColumnMeta>> columnMetaCache) {

        TableMapEventData tableMap = tableMapCache.get(deleteData.getTableId());
        if (tableMap == null) return List.of();

        List<Map<String, Object>> result = new ArrayList<>();

        for (Serializable[] before : deleteData.getRows()) {
            Map<String, Object> rowMap = new LinkedHashMap<>();
            for (int i = 0; i < before.length; i++) {
                rowMap.put("col" + i, before[i]);
            }
            result.add(rowMap);
        }

        return result;
    }


    public void handleBinlogMessage(TableMapEventData tableMap, List<BufferedEvent> buffer, String eventType) {
        // 가공 + HttpSender 호출
        log.info("Sending {} events to external system: table={}", buffer.size(), tableMap.getTable());
        new BinlogHttpSender(buffer.toString(), "http://target-system/api").start();
    }
}
