package com.sync.demo.biz.sqsSubscriber;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import com.sync.demo.biz.sqsSubscriber.factory.DBJobInterface;
import com.sync.demo.biz.sqsSubscriber.factory.TableJobFactory;
import com.sync.demo.biz.sqsSubscriber.mapper.SqsCommonMapper;
import com.sync.demo.biz.sqsSubscriber.service.meta.SqsDto;
import com.sync.demo.biz.sqsSubscriber.service.meta.SqsMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
@Slf4j
@Service
public class SqsMethod extends SqsSubMethod {

    public boolean checkMsg(SqsDto sqsDto) {
        try {
            JsonArray arr = JsonParser.parseString(sqsDto.getSqsString()).getAsJsonArray();
            sqsDto.setSqsMessages(new Gson().fromJson(arr, new TypeToken<List<SqsMessage>>(){}.getType()));
            return true;
        } catch (Exception e) {
            log.error("잘못된 메시지 형식", e);
            return false;
        }
    }

    public void reciveDBJob(SqsDto sqsDto) throws Exception {
        for (SqsMessage sqsMessage : sqsDto.getSqsMessages()) {
            doDBJob(sqsMessage);
        }
    }

    public void doDBJob(SqsMessage sqsMessage) throws Exception {
        switch (sqsMessage.getEventType()) {
            case "INSERT" -> normalInsert(sqsMessage);
            case "UPDATE" -> normalUpdate(sqsMessage);
            case "DELETE" -> normalDelete(sqsMessage);
            default -> throw new Exception("지원하지 않는 이벤트");
        }
    }
}

