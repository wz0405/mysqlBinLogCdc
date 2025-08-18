package com.sync.demo.biz.sqsSubscriber.meta;

import lombok.Data;


@Data
public class ColumInfo {
    private String tableName;         // 테이블명
    private String columnName;        // 실제 컬럼명 (snake_case)
    private String camelCaseName;     // camelCase 변환된 컬럼명
    private String columnType;        // 데이터 타입 (varchar(10), int 등)
    private String columnComment;     // 컬럼 주석
    private boolean nullable;         // NULL 허용 여부
    private boolean hasDefault;       // 기본값 존재 여부
    private boolean primaryKey;       // PK 여부 (옵션)
    private Integer maxLength;        // 최대 길이 (nullable)
    private Integer ordinalPosition;  // 컬럼 순서 (정렬용)
    private boolean autoIncrement;    //
}


