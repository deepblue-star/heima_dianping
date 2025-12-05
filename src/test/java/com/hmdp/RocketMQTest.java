package com.hmdp;

import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;


@SpringBootTest
public class RocketMQTest {
    @Autowired
    private RocketMQTemplate rocketMQTemplate;

    @Test
    void testSyncSend() {
        // 同步发送
        SendResult result = rocketMQTemplate.syncSend("TestTopic", "这是一条同步测试消息");
        System.out.println("同步消息发送成功，消息ID：" + result.getMsgId());
        // 此处可以添加断言，例如：
        Assertions.assertEquals(SendStatus.SEND_OK, result.getSendStatus());
    }

    @Test
    void testAsyncSend() throws InterruptedException {
        // 异步发送
        rocketMQTemplate.asyncSend("TestTopic", "这是一条异步测试消息", new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                System.out.println("异步消息发送成功！");
            }

            @Override
            public void onException(Throwable throwable) {
                System.err.println("异步消息发送失败：" + throwable.getMessage());
            }
        });
        // 等待一下，确保回调函数有机会执行
        Thread.sleep(3000);
    }

    @Test
    void testOneWaySend() {
        // 单向发送
        rocketMQTemplate.sendOneWay("TestTopic", "这是一条单向测试消息");
        System.out.println("单向消息已发送（不保证成功）");
    }
}
