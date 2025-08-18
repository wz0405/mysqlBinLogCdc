package com.sync.demo.biz.binlogClient.meta;

import lombok.Data;

@Data
public class ColumnMeta {
    private String tableName;          // 테이블명
    private String columnName;         // DB 원본 컬럼명 (snake_case)
    private String camelCaseName;      // camelCase 변환 컬럼명
    private boolean primaryKey;        // PK 여부
    private boolean nullable;          // NULL 허용 여부
    private int ordinalPosition;       // 컬럼 순서
    private Integer maxLength;         // 최대 문자 길이 (nullable)
    private String columnType;         // 컬럼 타입 (VARCHAR, INT 등)
    private String columnComment;      // 컬럼 주석
    private boolean hasDefault;        // 기본값 존재 여부
}