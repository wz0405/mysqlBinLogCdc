package com.sync.demo.biz.sqsSubscriber.mapper;

import com.sync.demo.biz.sqsSubscriber.service.meta.SqsMessage;
import org.apache.ibatis.annotations.Mapper;


@Mapper
public interface SqsCommonMapper {

    int insertTables(SqsMessage sqsMessage);
    int updateTables(SqsMessage sqsMessage);
    int deleteTables(SqsMessage sqsMessage);
    int isDupCheck(SqsMessage sqsMessage);



}
