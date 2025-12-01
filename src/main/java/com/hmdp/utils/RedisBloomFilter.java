package com.hmdp.utils;

import com.baomidou.mybatisplus.core.toolkit.CollectionUtils;
import com.google.common.hash.Funnels;
import com.google.common.hash.Hashing;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class RedisBloomFilter {
    private StringRedisTemplate redisTemplate;
    private String bloomFilterName; // 布隆过滤器在 Redis 中的 Key
    private long numBits;           // 位数组的长度 m
    private int numHashFunctions;   // 哈希函数的数量 k

    /**
     * 构造函数
     * @param redisTemplate Spring Data Redis 的 StringRedisTemplate
     * @param bloomFilterName 布隆过滤器在 Redis 中的 Key
     * @param expectedInsertions 预期插入的元素数量 n
     * @param falsePositiveProbability 可接受的误判率 p
     */
    public RedisBloomFilter(StringRedisTemplate redisTemplate, String bloomFilterName, long expectedInsertions, double falsePositiveProbability) {
        this.redisTemplate = redisTemplate;
        this.bloomFilterName = bloomFilterName;

        // 计算最优的位数组长度 m
        this.numBits = calculateOptimalNumBits(expectedInsertions, falsePositiveProbability);
        // 计算最优的哈希函数数量 k
        this.numHashFunctions = calculateOptimalNumHashFunctions(expectedInsertions, numBits);

        System.out.println("Bloom Filter: " + bloomFilterName + " initialized with:");
        System.out.println("  Expected insertions (n): " + expectedInsertions);
        System.out.println("  False positive probability (p): " + falsePositiveProbability);
        System.out.println("  Calculated numBits (m): " + numBits);
        System.out.println("  Calculated numHashFunctions (k): " + numHashFunctions);
    }

    /**
     * 布隆过滤器公式，计算最优的位数组长度 m
     * m = -(n * ln p) / (ln 2)^2
     */
    private long calculateOptimalNumBits(long expectedInsertions, double falsePositiveProbability) {
        return (long) (-expectedInsertions * Math.log(falsePositiveProbability) / (Math.log(2) * Math.log(2)));
    }

    /**
     * 布隆过滤器公式，计算最优的哈希函数数量 k
     * k = (m / n) * ln 2
     */
    private int calculateOptimalNumHashFunctions(long expectedInsertions, long numBits) {
        return Math.max(1, (int) Math.round((double) numBits / expectedInsertions * Math.log(2)));
    }

    /**
     * 添加元素到布隆过滤器
     * @param element 要添加的元素
     */
    public void add(String element) {
        if (element == null) {
            return;
        }

        // 使用多个哈希函数计算出多个哈希值
        long[] hashValues = generateHashValues(element);

        // 使用Pipeline批量设置位，只需一次网络IO
        redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            byte[] keyBytes = bloomFilterName.getBytes();
            for (long hashValue : hashValues) {
                connection.setBit(keyBytes, hashValue, true);
            }
            return null;
        });
    }

    /**
     * 添加元素到布隆过滤器
     * @param elements 要添加的元素列表
     */
    public void batchAdd(List<String> elements) {
        if (CollectionUtils.isEmpty(elements)) {
            return;
        }

        // 收集所有需要设置的位（去重）
        Set<Long> allBitPositions = new HashSet<>();

        // 为每个元素计算哈希值并收集所有位位置
        for (String element : elements) {
            if (element == null) {
                continue;
            }

            // 计算当前元素的哈希值
            long[] hashValues = generateHashValues(element);

            // 将所有位位置添加到集合中（自动去重）
            for (long hashValue : hashValues) {
                allBitPositions.add(hashValue);
            }

            // 使用Pipeline批量设置位，只需一次网络IO
            redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
                byte[] keyBytes = bloomFilterName.getBytes();
                for (Long bitPosition : allBitPositions) {
                    connection.setBit(keyBytes, bitPosition, true);
                }
                return null;
            });
        }
    }

    /**
     * 判断元素是否存在于布隆过滤器中
     * @param element 要查询的元素
     * @return true 可能存在，false 一定不存在
     */
    public boolean mightContain(String element) {
        long[] hashValues = generateHashValues(element);
        // 检查所有对应的位是否都为 1
        for (long hashValue : hashValues) {
            Boolean bit = redisTemplate.opsForValue().getBit(bloomFilterName, hashValue);
            if (bit == null || !bit) {
                return false; // 只要有一个位为 0，则一定不存在
            }
        }
        return true; // 所有位都为 1，则可能存在
    }

    /**
     * 生成给定元素的 k 个哈希值
     * 这里使用 Guava 的 MurmurHash3 算法，该算法具有很好的均匀性和低冲突性
     * @param element 元素
     * @return 包含 k 个哈希值的数组
     */
    private long[] generateHashValues(String element) {
        long[] hashValues = new long[numHashFunctions];
        // 使用不同种子值的Guava的MurmurHash3_128算法生成两个基础哈希值
        // 种子值0和0x1234ABCD确保两个哈希函数在统计上独立
        long hash1 = Hashing.murmur3_128(0).hashObject(element, Funnels.stringFunnel(StandardCharsets.UTF_8)).asLong();
        long hash2 = Hashing.murmur3_128(0x1234ABCD).hashObject(element, Funnels.stringFunnel(StandardCharsets.UTF_8)).asLong(); // 另一个哈希函数

        // 使用Kirsch-Mitzenmacher双哈希技术生成k个哈希值
        // 该技术通过两个基础哈希值模拟多个哈希函数，避免计算k个独立哈希的开销
        // 公式：g(i, x) = (h1(x) + i * h2(x)) mod m
        // 其中：i是哈希索引(0到k-1)，m是位数组长度
        for (int i = 0; i < numHashFunctions; i++) {
            hashValues[i] = Math.abs((hash1 + i * hash2) % numBits);
        }
        return hashValues;
    }

    /**
     * 清空布隆过滤器（谨慎操作，会清除所有数据）
     */
    public void clear() {
        redisTemplate.delete(bloomFilterName);
    }
}
