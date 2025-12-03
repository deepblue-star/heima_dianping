package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.StringUtils;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.hmdp.utils.RedisBloomFilter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

//    @Resource
//    private ShopBloomFilter shopBloomFilter;

    @Autowired
    @Qualifier("shopBloomFilter")
    private RedisBloomFilter shopBloomFilter;

    @Override
    public Result queryById(Long id) {
        // 1. 检查布隆过滤器是否存在该商家id
        if (!shopBloomFilter.mightContain(id.toString())) {
            return Result.fail("布隆过滤器检查店铺不存在！");
        }

        // 2. Redis查商家缓存
        String key = CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);

        // 3. 存在非空值，说明命中，直接返回
        if (StringUtils.isNotBlank(shopJson)) {
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return Result.ok(shop);
        }

        // 4. 不存在，获取Redis互斥锁查DB
        String lockKey = LOCK_SHOP_KEY + id;
        boolean lockAcquired = false;

        try {
            // 尝试次数
            int tryCnt = 0;

            // 5. 判断是否获取成功，最多尝试获取5次锁
            while (!lockAcquired && tryCnt < 5) {
                // 失败则休眠重试
                lockAcquired = tryLock(lockKey);
                if (lockAcquired) {
                    break;
                }
                tryCnt++;
                Thread.sleep(50);
            }


            if (!lockAcquired) {
                return Result.fail("获取互斥锁失败！");
            }

            log.debug("获取互斥锁成功");

            // 6. 获取锁成功后再次检查缓存（DCL的第二重检查）
            shopJson = stringRedisTemplate.opsForValue().get(key);
            if (StringUtils.isNotBlank(shopJson)) {
                Shop shop = JSONUtil.toBean(shopJson, Shop.class);
                return Result.ok(shop);
            }

            // 7. 成功，则查DB
            Shop shop = getById(id);
            if (shop == null) {
                return Result.fail("店铺不存在！");
            }

            // 8. 存在，写入Redis缓存
            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
            return Result.ok(shop);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            // 9. 释放Redis互斥锁
            if (lockAcquired) {
                unlock(lockKey);
            }
        }
    }

    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 5, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }

    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("店铺id不能为空");
        }
        String key = CACHE_SHOP_KEY + id;

        // 1. 更新DB
        updateById(shop);

        // 2. 删除Redis缓存
        stringRedisTemplate.delete(key);
        return Result.ok();
    }

    @Transactional
    @Override
    public Result saveShop(Shop shop) {
        // 新增记录到DB
        save(shop);
        // 添加到布隆过滤器
        shopBloomFilter.add(shop.getId().toString());
        // 返回店铺id
        return Result.ok(shop.getId());
    }

    @Override
    public void loadAllShopIdToBloomFilter() {
        int offset = 0;
        int batchSize = 10000;
        List<String> shopIds = new ArrayList<>();
        int totalCnt = 0;

        // 循环批量查询所有商家id
        do {
            // 创建分页对象
            Page<Shop> page = new Page<>(offset, offset + batchSize);

            // 构建查询条件，只查询ID字段
            QueryWrapper<Shop> queryWrapper = new QueryWrapper<>();
            queryWrapper.select("id");

            // 执行分页查询
            Page<Shop> shopPage = page(page, queryWrapper);

            shopIds = shopPage.getRecords().stream()
                    .map(Shop::getId)
                    .map(Object::toString)
                    .collect(Collectors.toList());

            if (!shopIds.isEmpty()) {
                shopBloomFilter.batchAdd(shopIds);
                totalCnt += shopIds.size();
            }
        } while (shopIds.size() == batchSize);
        log.debug("初始化商家id到布隆过滤器，共 {} 条数据", totalCnt);
    }
}
