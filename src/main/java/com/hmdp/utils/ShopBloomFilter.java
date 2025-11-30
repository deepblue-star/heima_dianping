package com.hmdp.utils;

import com.baomidou.mybatisplus.core.toolkit.CollectionUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.List;

import static com.hmdp.utils.RedisConstants.SHOP_BLOOM_FILTER_KEY;

@Component
@Slf4j
public class ShopBloomFilter {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 初始化商家布隆过滤器（应用启动时调用）
     */
    @PostConstruct
    public void initShopBloomFilter() {
        // 检查布隆过滤器是否已存在，存在则返回
        Boolean exists = stringRedisTemplate.hasKey(SHOP_BLOOM_FILTER_KEY);
        if (exists) {
            return;
        }
        // 预估用户总量：100w，误判率1%
        long expectedInsertions = 1_000_000L;
        double falsePositiveProbability = 0.01;
        // execute是要给底层API，用于执行高级API不支持的Redis命令
        stringRedisTemplate.execute(new RedisCallback<Object>() {
            @Override
            public Object doInRedis(RedisConnection connection) {
                log.debug("布隆过滤器开始初始化...");
                // 获取一个Redis连接，执行布隆过滤器的初始化命令
                Object result = connection.execute("BF.RESERVE",
                        SHOP_BLOOM_FILTER_KEY.getBytes(),
                        String.valueOf(falsePositiveProbability).getBytes(),
                        String.valueOf(expectedInsertions).getBytes()
                );
                log.debug("布隆过滤器已初始化, result: {}", result);
                return result;
            }
        });
    }

    /**
     * 添加商家id到布隆过滤器
     * @param shopId
     * @return true表示成功
     */
    public Boolean addShopToBloomFilter(Long shopId) {
        if (shopId == null) {
            return false;
        }
        Boolean executeResult = stringRedisTemplate.execute(new RedisCallback<Boolean>() {
            @Override
            public Boolean doInRedis(RedisConnection connection) {
                Object execute = connection.execute("BF.ADD",
                        SHOP_BLOOM_FILTER_KEY.getBytes(),
                        shopId.toString().getBytes()
                );
                return (Boolean) execute;
            }
        });
        return executeResult;
    }

    /**
     * 批量添加商家id到布隆过滤器
     * 使用批量操作减少和Redis交互的网络IO耗时
     * @param shopIds
     * @return 返回添加成功的数量
     */
    public Integer batchAddShopToBloomFilter(List<Long> shopIds) {
        if (CollectionUtils.isEmpty(shopIds)) {
            return 0;
        }

        return stringRedisTemplate.execute(new RedisCallback<Integer>() {
            @Override
            public Integer doInRedis(RedisConnection connection) {
                // 准备BF.MADD命令的参数
                byte[][] params = new byte[shopIds.size() + 1][];
                // 第一个参数是布隆过滤器的键名
                params[0] = SHOP_BLOOM_FILTER_KEY.getBytes();

                // 后续参数是要添加的元素
                for (int i = 0; i < shopIds.size(); i++) {
                    params[i + 1] = shopIds.get(i).toString().getBytes();
                }

                // 执行BF.MADD命令批量添加
                Object result = connection.execute("BF.MADD", params);

                // BF.MADD返回一个数组，包含每个元素的添加结果
                int successCnt = 0;
                if (result instanceof List) {
                    List<?> resultList = (List<?>) result;
                    // 检查结果是否为1（成功添加）
                    for (Object item : resultList) {
                        if ("1".equals(item.toString())) {
                            successCnt++;
                        }
                    }
                    return successCnt;
                }
                return 0;
            }
        });
    }

    /**
     * 检查商家id是否存在于布隆过滤器
     * @param shopId
     * @return 如果存在则返回true，否则返回false
     */
    public boolean shopExists(String shopId) {
        Boolean executeResult = stringRedisTemplate.execute(new RedisCallback<Boolean>() {
            @Override
            public Boolean doInRedis(RedisConnection connection) {
                Object execute = connection.execute("BF.EXISTS",
                        SHOP_BLOOM_FILTER_KEY.getBytes(),
                        shopId.getBytes()
                );
                return (Boolean) execute;
            }
        });
        return executeResult;
    }
}
