package com.hmdp.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RedissonConfig {

    @Value("${spring.redis.host}")
    private String REDIS_IP_ADDRESS;

    @Value("${spring.redis.port}")
    private String REDIS_PORT;

    @Bean
    public RedissonClient redissonClient() {
        Config config = new Config();
        config.useSingleServer().setAddress("redis://" + REDIS_IP_ADDRESS + ":" + REDIS_PORT);
        return Redisson.create(config);
    }
}
