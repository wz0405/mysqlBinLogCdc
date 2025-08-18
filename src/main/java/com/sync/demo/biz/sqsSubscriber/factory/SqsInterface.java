package com.sync.demo.biz.sqsSubscriber.factory;

import com.sync.demo.biz.sqsSubscriber.service.meta.SqsDto;

import java.util.Map;


public interface SqsInterface {
	boolean beforProcess(SqsDto var1) throws Exception;
	void doProcess(SqsDto var1) throws Exception;
	Map afterProcess(SqsDto var1) throws Exception;
}
