package com.sync.demo.biz.binlogClient.method;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import com.sync.demo.biz.binlogClient.meta.BufferedEvent;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Configuration
public class BinlogClientListener extends BinlogClientMethod {

    private final Map<Long, TableMapEventData> tableMapCache = new HashMap<>();
    private final Map<String, List<BufferedEvent>> eventBuffer = new ConcurrentHashMap<>();
    private static final int BUFFER_SIZE_THRESHOLD = 10;

    // ====== 옵션: "SQS" or "HTTP" ======
    private static final String MODE = "SQS"; // ← 필요 시 "HTTP"로 변경

    private final SqsPublisher sqsPublisher;

    public BinlogClientListener(SqsPublisher sqsPublisher) {
        this.sqsPublisher = sqsPublisher;
    }

    @PostConstruct
    public void start() {
        new Thread(() -> {
            try {
                runBinlogClient();
            } catch (Exception e) {
                log.error("Binlog client error", e);
            }
        }, "Binlog-Client-Thread").start();
    }

    private void runBinlogClient() throws Exception {
        BinaryLogClient client = new BinaryLogClient("localhost", 3306, "user", "password");
        client.setServerId(new Random().nextInt(100000));

        client.registerEventListener(event -> {
            EventData data = event.getData();
            if (data instanceof TableMapEventData tableMap) {
                tableMapCache.put(tableMap.getTableId(), tableMap);
                return;
            }

            if (data instanceof WriteRowsEventData write) {
                var rows = extractInsertRows(write, tableMapCache, columnMetaCache);
                handleRowEvent(write.getTableId(), rows, "INSERT");
            } else if (data instanceof UpdateRowsEventData update) {
                var rows = extractUpdatedRows(update, tableMapCache, columnMetaCache);
                handleRowEvent(update.getTableId(), rows, "UPDATE");
            } else if (data instanceof DeleteRowsEventData delete) {
                var rows = extractDeletedRows(delete, tableMapCache, columnMetaCache);
                handleRowEvent(delete.getTableId(), rows, "DELETE");
            }
        });

        client.connect();
    }

    private void handleRowEvent(long tableId, List<Map<String, Object>> rows, String eventType) {
        TableMapEventData tableMap = tableMapCache.get(tableId);
        if (tableMap == null || rows.isEmpty()) return;

        log.info("[BINLOG] {} event detected: table={}, rowCount={}",
                eventType, tableMap.getTable(), rows.size());

        // SQS는 즉시 발송
        if ("SQS".equalsIgnoreCase(MODE)) {
            for (Map<String, Object> row : rows) {
                String messageId = UUID.randomUUID().toString();
                sqsPublisher.sendMessage("binlog-queue.fifo", messageId, row.toString());
            }
        }

        // HTTP는 버퍼링 후 flush
        if ("HTTP".equalsIgnoreCase(MODE)) {
            bufferAndMaybeFlush(tableMap, rows, eventType);
        }
    }

    private void bufferAndMaybeFlush(TableMapEventData tableMap, List<Map<String, Object>> rows, String eventType) {
        eventBuffer.compute(tableMap.getTable(), (key, buffer) -> {
            if (buffer == null) buffer = new ArrayList<>();
            for (Map<String, Object> row : rows) {
                buffer.add(new BufferedEvent(row, eventType));
            }

            if (buffer.size() >= BUFFER_SIZE_THRESHOLD) {
                flushBuffer(tableMap, buffer, eventType);
                return new ArrayList<>();
            }
            return buffer;
        });
    }

    /** 주기적으로 HTTP 잔여 buffer flush */
    @Scheduled(fixedRate = 1000)
    public void flushRemainingData() {
        if (!"HTTP".equalsIgnoreCase(MODE)) return; // HTTP 모드에서만 수행

        for (String table : eventBuffer.keySet()) {
            eventBuffer.compute(table, (key, buffer) -> {
                if (buffer == null || buffer.isEmpty()) return buffer;

                TableMapEventData tableMap = tableMapCache.values().stream()
                        .filter(t -> table.equals(t.getTable()))
                        .findFirst().orElse(null);

                if (tableMap != null) {
                    List<BufferedEvent> snapshot = new ArrayList<>(buffer);
                    log.info("[BINLOG] Scheduled flush: table={}, bufferedSize={}", table, snapshot.size());
                    flushBuffer(tableMap, snapshot, "SCHEDULED");
                }

                return new ArrayList<>(); // buffer 초기화
            });
        }
    }

    private void flushBuffer(TableMapEventData tableMap, List<BufferedEvent> buffer, String eventType) {
        log.info("[BINLOG] Flushing {} rows for table={} (eventType={})",
                buffer.size(), tableMap.getTable(), eventType);

        // HTTP 전송
        handleBinlogMessage(tableMap, buffer, eventType);
    }
}
