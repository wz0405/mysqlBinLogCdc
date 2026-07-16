package com.sync.demo.biz.binlogClient.method;

import com.github.shyiko.mysql.binlog.event.*;
import com.sync.demo.biz.binlogClient.mapper.BinlogClientMapper;
import com.sync.demo.biz.binlogClient.meta.BufferedEvent;
import com.sync.demo.biz.binlogClient.meta.ColumnMeta;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Component
public class BinlogClientMethod {

    protected final Map<String, Map<String, ColumnMeta>> columnMetaCache = new HashMap<>();

    @Autowired(required = false)
    private BinlogClientMapper binlogClientMapper;

    @Value("${spring.datasource.url:}")
    private String datasourceUrl;

    /**
     * 테이블의 컬럼 메타정보를 조회하고 캐싱
     */
    protected void loadColumnMetadata(String dbName, String tableName) {
        if (columnMetaCache.containsKey(tableName)) {
            return; // 이미 캐시됨
        }

        if (binlogClientMapper == null) {
            log.warn("[COLUMN-META] BinlogClientMapper 미주입, 컬럼명 매핑 불가");
            return;
        }

        try {
            List<Map<String, Object>> columns = binlogClientMapper.selectTableColumns(dbName, List.of(tableName));
            Map<String, ColumnMeta> colMap = new LinkedHashMap<>();

            // ORDINAL_POSITION 순으로 정렬
            columns.stream()
                    .sorted((a, b) -> {
                        int posA = ((Number) a.getOrDefault("ORDINAL_POSITION", 0)).intValue();
                        int posB = ((Number) b.getOrDefault("ORDINAL_POSITION", 0)).intValue();
                        return Integer.compare(posA, posB);
                    })
                    .forEach(col -> {
                        ColumnMeta meta = new ColumnMeta();
                        meta.setTableName(tableName);
                        meta.setColumnName((String) col.get("COLUMN_NAME"));
                        meta.setOrdinalPosition(((Number) col.get("ORDINAL_POSITION")).intValue());
                        meta.setColumnType((String) col.get("COLUMN_TYPE"));
                        meta.setNullable("YES".equalsIgnoreCase((String) col.get("IS_NULLABLE")));
                        meta.setPrimaryKey("PRI".equalsIgnoreCase((String) col.get("COLUMN_KEY")));
                        meta.setColumnComment((String) col.get("COLUMN_COMMENT"));
                        if (col.get("CHARACTER_MAXIMUM_LENGTH") != null) {
                            meta.setMaxLength(((Number) col.get("CHARACTER_MAXIMUM_LENGTH")).intValue());
                        }
                        colMap.put(meta.getColumnName(), meta);
                    });

            columnMetaCache.put(tableName, colMap);
            log.info("[COLUMN-META] 테이블 {} 컬럼 {} 개 캐싱", tableName, colMap.size());
        } catch (Exception e) {
            log.error("[COLUMN-META] 조회 실패: table={}, db={}", tableName, dbName, e);
        }
    }

    /**
     * 행 데이터를 실제 컬럼명으로 매핑
     */
    protected Map<String, Object> mapRowToColumns(Serializable[] row, String tableName, Map<String, ColumnMeta> colMap) {
        Map<String, Object> rowMap = new LinkedHashMap<>();

        if (colMap == null || colMap.isEmpty()) {
            // 캐시가 없으면 col0, col1... 사용 (fallback)
            for (int i = 0; i < row.length; i++) {
                rowMap.put("col" + i, row[i]);
            }
            return rowMap;
        }

        List<String> colNames = new ArrayList<>(colMap.keySet());
        if (colNames.size() != row.length) {
            log.warn("[COLUMN-MISMATCH] 테이블 {} 캐시 컬럼 수({}) != row 길이({}), 캐시 리로드",
                    tableName, colNames.size(), row.length);
            columnMetaCache.remove(tableName); // 캐시 무효화
            // fallback: col0, col1...
            for (int i = 0; i < row.length; i++) {
                rowMap.put("col" + i, row[i]);
            }
            return rowMap;
        }

        // 실제 컬럼명으로 매핑
        for (int i = 0; i < row.length; i++) {
            String colName = colNames.get(i);
            rowMap.put(colName, row[i]);
        }

        return rowMap;
    }

    protected List<Map<String, Object>> extractInsertRows(
            WriteRowsEventData writeData,
            Map<Long, TableMapEventData> tableMapCache,
            Map<String, Map<String, ColumnMeta>> columnMetaCache) {

        TableMapEventData tableMap = tableMapCache.get(writeData.getTableId());
        if (tableMap == null) return List.of();

        String tableName = tableMap.getTable();
        String dbName = extractDatabaseName();

        // 컬럼 메타 로드
        loadColumnMetadata(dbName, tableName);
        Map<String, ColumnMeta> colMap = columnMetaCache.get(tableName);

        List<Map<String, Object>> result = new ArrayList<>();
        for (Serializable[] row : writeData.getRows()) {
            Map<String, Object> rowMap = mapRowToColumns(row, tableName, colMap);
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

        String tableName = tableMap.getTable();
        String dbName = extractDatabaseName();

        // 컬럼 메타 로드
        loadColumnMetadata(dbName, tableName);
        Map<String, ColumnMeta> colMap = columnMetaCache.get(tableName);

        List<Map<String, Object>> result = new ArrayList<>();

        for (Map.Entry<Serializable[], Serializable[]> row : updateData.getRows()) {
            Serializable[] before = row.getKey();
            Serializable[] after = row.getValue();

            Map<String, Object> rowMap = new LinkedHashMap<>();

            if (colMap == null || colMap.isEmpty()) {
                // fallback
                for (int i = 0; i < after.length; i++) {
                    rowMap.put("col" + i + "_before", before[i]);
                    rowMap.put("col" + i + "_after", after[i]);
                }
            } else {
                List<String> colNames = new ArrayList<>(colMap.keySet());
                if (colNames.size() != after.length) {
                    log.warn("[COLUMN-MISMATCH] UPDATE 테이블 {} 캐시 컬럼 수({}) != row 길이({}), 캐시 리로드",
                            tableName, colNames.size(), after.length);
                    columnMetaCache.remove(tableName);
                    for (int i = 0; i < after.length; i++) {
                        rowMap.put("col" + i + "_before", before[i]);
                        rowMap.put("col" + i + "_after", after[i]);
                    }
                } else {
                    for (int i = 0; i < after.length; i++) {
                        String colName = colNames.get(i);
                        rowMap.put(colName + "_before", before[i]);
                        rowMap.put(colName + "_after", after[i]);
                    }
                }
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

        String tableName = tableMap.getTable();
        String dbName = extractDatabaseName();

        // 컬럼 메타 로드
        loadColumnMetadata(dbName, tableName);
        Map<String, ColumnMeta> colMap = columnMetaCache.get(tableName);

        List<Map<String, Object>> result = new ArrayList<>();

        for (Serializable[] before : deleteData.getRows()) {
            Map<String, Object> rowMap = mapRowToColumns(before, tableName, colMap);
            result.add(rowMap);
        }

        return result;
    }

    /**
     * datasourceUrl에서 데이터베이스명 추출
     * jdbc:mysql://host:port/dbname 형식
     */
    private String extractDatabaseName() {
        if (datasourceUrl == null || datasourceUrl.isEmpty()) {
            return "testDB"; // 기본값
        }
        try {
            // jdbc:mysql://host:port/dbname?...
            String[] parts = datasourceUrl.split("/");
            if (parts.length >= 4) {
                String dbPart = parts[3];
                if (dbPart.contains("?")) {
                    dbPart = dbPart.substring(0, dbPart.indexOf("?"));
                }
                return dbPart;
            }
        } catch (Exception e) {
            log.warn("[DB-NAME] datasourceUrl 파싱 실패", e);
        }
        return "testDB";
    }


    public void handleBinlogMessage(TableMapEventData tableMap, List<BufferedEvent> buffer, String eventType) {
        // 가공 + HttpSender 호출
        log.info("Sending {} events to external system: table={}", buffer.size(), tableMap.getTable());
        new BinlogHttpSender(buffer.toString(), "http://target-system/api").start();
    }
}
