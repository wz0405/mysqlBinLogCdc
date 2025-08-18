package com.sync.demo.biz.sqsSubscriber.service;

import com.sync.demo.biz.sqsSubscriber.service.meta.SqsDto;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.sync.demo.biz.sqsSubscriber.SqsMethod;
import com.sync.demo.biz.sqsSubscriber.factory.SqsInterface;

import java.util.Map;

@Service
public class SqsSubscribeJob implements SqsInterface {

    @Autowired
    SqsMethod sqsMethod;

    @Override
    public boolean beforProcess(SqsDto sqsDto) {
        return sqsMethod.checkMsg(sqsDto);
    }

    @Override
    public void doProcess(SqsDto sqsDto) throws Exception {
        sqsMethod.reciveDBJob(sqsDto);
    }

    @Override
    public Map afterProcess(SqsDto sqsDto) {
        return null; // Demo: 생략
    }
}
