package com.sync.demo.biz.sqsSubscriber.factory;

import com.sync.demo.biz.sqsSubscriber.service.meta.SqsMessage;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

@Service
public class TableJobFactory {

    public DBJobInterface getBean(ApplicationContext appCtx, SqsMessage sqsMessage) throws Exception {

        return  (DBJobInterface)appCtx.getBean(sqsMessage.getTableName());
   }
}

