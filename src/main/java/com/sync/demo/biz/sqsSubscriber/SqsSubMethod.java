package com.sync.demo.biz.sqsSubscriber;

import com.sync.demo.biz.sqsSubscriber.service.meta.SqsMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import com.sync.demo.biz.sqsSubscriber.mapper.SqsCommonMapper;
import com.sync.demo.biz.sqsSubscriber.meta.ColumInfo;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
public class SqsSubMethod {

    @Autowired
    private SqsCommonMapper sqsCommonMapper;

    @Value("${subscribe.target.db}")
    private String targetDB;

    /** INSERT */
    protected void normalInsert(SqsMessage msg) {
        msg.setTargetDB(targetDB);
        sqsCommonMapper.insertTables(msg);
        log.info("[DB-INSERT] table={}, data={}", msg.getTableName(), msg.getDataSourceMap());
    }

    /** UPDATE */
    protected void normalUpdate(SqsMessage msg) {
        msg.setTargetDB(targetDB);
        sqsCommonMapper.updateTables(msg);
        log.info("[DB-UPDATE] table={}, data={}", msg.getTableName(), msg.getDataSourceMap());
    }

    /** DELETE */
    protected void normalDelete(SqsMessage msg) {
        msg.setTargetDB(targetDB);
        sqsCommonMapper.deleteTables(msg);
        log.info("[DB-DELETE] table={}, data={}", msg.getTableName(), msg.getPkSourceMap());
    }

    /** UPSERT (Insert or Update) */
    protected void normalUpsert(SqsMessage msg) {
        msg.setTargetDB(targetDB);
        int count = sqsCommonMapper.isDupCheck(msg);

        if (count == 0) {
            normalInsert(msg);
        } else {
            normalUpdate(msg);
        }
        log.info("[DB-UPSERT] table={}, data={}", msg.getTableName(), msg.getDataSourceMap());
    }
}

