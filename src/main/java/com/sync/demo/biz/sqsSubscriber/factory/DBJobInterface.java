package com.sync.demo.biz.sqsSubscriber.factory;

import com.sync.demo.biz.sqsSubscriber.service.meta.SqsMessage;

public interface DBJobInterface {

    public void insert(SqsMessage sqsMessage)throws Exception;

    public void delete(SqsMessage sqsMessage)throws Exception;

    public void update(SqsMessage sqsMessage)throws Exception;

    public void upsert(SqsMessage sqsMessage)throws Exception;
}
