package com.hmdp.service.impl;

import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.StringUtils;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.hmdp.utils.ShopBloomFilter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;
import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TTL;

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

    @Resource
    private ShopBloomFilter shopBloomFilter;

    @Override
    public Result queryById(Long id) {
        // 1. 检查布隆过滤器是否存在该商家id
        if (!shopBloomFilter.shopExists(id.toString())) {
            return Result.fail("店铺不存在！");
        }

        // 2. Redis查商家缓存
        String key = CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);

        // 3. 存在直接返回
        if (StringUtils.isNotBlank(shopJson)) {
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return Result.ok(shop);
        }

        // 4. 不存在，查DB
        Shop shop = getById(id);

        // 5. 不存在，返回错误
        if (shop == null) {
            return Result.fail("店铺不存在！");
        }

        // 6. 存在，写入Redis缓存
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        return Result.ok(shop);
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

    @Override
    public void loadAllShopIdToBloomFilter() {
        int offset = 0;
        int batchSize = 10000;
        List<Long> shopIds = new ArrayList<>();
        int totalCnt = 0;
        int totoalSuccessCnt = 0;

        // 循环批量查询所有商家id
        do {
            // 创建分页对象
            Page<Shop> page = new Page<>(offset, offset + batchSize);

            // 构建查询条件，只查询ID字段
            LambdaQueryWrapper<Shop> queryWrapper = new LambdaQueryWrapper<>();
            queryWrapper.select(Shop::getId);

            // 执行分页查询
            Page<Shop> shopPage = page(page, queryWrapper);

            shopIds = shopPage.getRecords().stream()
                    .map(Shop::getId)
                    .collect(Collectors.toList());

            if (!shopIds.isEmpty()) {
                Integer successCnt = shopBloomFilter.batchAddShopToBloomFilter(shopIds);
                totalCnt += shopIds.size();
                totoalSuccessCnt += successCnt;
            }
        } while (shopIds.size() == batchSize);
        log.debug("初始化商家id到布隆过滤器，共 {} 条数据, 成功 {} 条", totalCnt, totoalSuccessCnt);
    }
}
