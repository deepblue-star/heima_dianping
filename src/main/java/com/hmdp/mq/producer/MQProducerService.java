package com.hmdp.mq.producer;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class MQProducerService {

    @Autowired
    @Qualifier("seckillRocketMQTemplate")
    private RocketMQTemplate seckillRocketMQTemplate;

    public SendResult sendSeckillMsgSync(String topic, String msgBody) {
        SendResult sendResult = seckillRocketMQTemplate.syncSend(topic, MessageBuilder.withPayload(msgBody).build());
        log.info("[sendSeckillMsgSync] topic={}, sendResult={}", topic, JSON.toJSONString(sendResult));
        return sendResult;
    }
}
