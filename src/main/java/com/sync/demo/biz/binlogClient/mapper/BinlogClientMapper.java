package com.sync.demo.biz.binlogClient.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;
import java.util.Map;

@Mapper
public interface BinlogClientMapper {
    List<Map<String, Object>> selectTableColumns(
            @Param("dbName") String dbName,
            @Param("tableList") List<String> tableList
    );
}