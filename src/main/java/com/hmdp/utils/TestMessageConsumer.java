package com.hmdp.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;


@Component
@RocketMQMessageListener(topic = "TestTopic", consumerGroup = "test-consumer-group")
@Slf4j
public class TestMessageConsumer implements RocketMQListener<MessageExt> {
    @Override
    public void onMessage(MessageExt message) {
        String body = new String(message.getBody()); // 获取消息体内容
        String msgId = message.getMsgId(); // 获取消息ID
        String tags = message.getTags(); // 获取消息标签(Tag)
        log.debug("收到消息ID: " + msgId + ", 内容: " + body);
//        System.out.println("收到消息ID: " + msgId + ", 内容: " + body);
    }
}
