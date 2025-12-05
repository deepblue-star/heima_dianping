package com.hmdp.mq.consumer;

import cn.hutool.json.JSONUtil;
import com.hmdp.mq.dto.CreateOrderDTO;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

import static com.hmdp.utils.MQConstants.SECKILL_VOUCHER_ORDER_CONSUMER_GROUP;
import static com.hmdp.utils.MQConstants.SECKILL_VOUCHER_ORDER_TOPIC;

@Component
@RocketMQMessageListener(topic = SECKILL_VOUCHER_ORDER_TOPIC, consumerGroup = SECKILL_VOUCHER_ORDER_CONSUMER_GROUP)
@Slf4j
public class SeckillVoucherOrderConsumer implements RocketMQListener<MessageExt> {
    @Resource
    private IVoucherOrderService voucherOrderService;

    @Override
    public void onMessage(MessageExt message) {
        byte[] body = message.getBody();
        String msg = new String(body);
        log.debug("接受到秒杀订单创建消息: {}", msg);
        CreateOrderDTO createOrderDTO = JSONUtil.toBean(msg, CreateOrderDTO.class);
        if(message.getReconsumeTimes() == 3){
            log.error("{}消费了3次都消费失败",createOrderDTO.toString());
            //消息入库，人工干预
        }
        log.info("createOrderDTO:{}",createOrderDTO.toString());
        boolean success = voucherOrderService.createVoucherOrder(createOrderDTO.getUserId(), createOrderDTO.getVoucherId(), createOrderDTO.getOrderId());
        if (!success) {
            log.error("voucherId: {} 接受到秒杀订单创建消息失败", createOrderDTO.getVoucherId());
        }
    }
}
