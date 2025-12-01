package com.hmdp.config;

import com.hmdp.utils.RedisBloomFilter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;

import static com.hmdp.utils.RedisConstants.BLOOM_SHOP_KEY;

@Configuration
public class RedisConfig {
    /**
     * 注册一个商家布隆过滤器
     * 假设我们预期有 1000 万个项，误判率为 0.01%
     */
    @Bean
    public RedisBloomFilter shopBloomFilter(StringRedisTemplate redisTemplate) {
        String bloomFilterName = BLOOM_SHOP_KEY; // Redis Key
        long expectedInsertions = 10_000_000L; // 预期数量
        double falsePositiveProbability = 0.0001; // 0.01% 误判率
        return new RedisBloomFilter(redisTemplate, bloomFilterName, expectedInsertions, falsePositiveProbability);
    }
}
