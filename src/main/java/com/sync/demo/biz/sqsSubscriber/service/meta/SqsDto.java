package com.sync.demo.biz.sqsSubscriber.service.meta;

import lombok.Data;

import java.util.List;

@Data
public class SqsDto {
    private List<SqsMessage> sqsMessages;
    private String sqsString;
}