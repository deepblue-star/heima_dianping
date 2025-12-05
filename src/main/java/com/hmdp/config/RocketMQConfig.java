package com.hmdp.config;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static com.hmdp.utils.MQConstants.SECKILL_VOUCHER_ORDER_PRODUCER_GROUP;

@Configuration
public class RocketMQConfig {
    @Value("${rocketmq.name-server}")
    private String nameServer;

    @Bean
    public RocketMQTemplate seckillRocketMQTemplate() {
        RocketMQTemplate template = new RocketMQTemplate();
        DefaultMQProducer producer = new DefaultMQProducer(SECKILL_VOUCHER_ORDER_PRODUCER_GROUP);
        producer.setNamesrvAddr(nameServer);
        template.setProducer(producer);
        return template;
    }
}
