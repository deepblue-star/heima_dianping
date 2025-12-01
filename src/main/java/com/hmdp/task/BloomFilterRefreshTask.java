package com.hmdp.task;

import com.hmdp.service.IShopService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class BloomFilterRefreshTask {
    @Autowired
    private IShopService shopService;

    /**
     * 应用启动完成后初始化布隆过滤器
     * 使用ApplicationReadyEvent确保所有Bean都已初始化完成
     */
    @EventListener(ApplicationReadyEvent.class)
    public void initBloomFilterOnStartup() {
        log.info("应用启动完成，开始初始化布隆过滤器...");
        try {
            shopService.loadAllShopIdToBloomFilter();
            log.info("布隆过滤器初始化完成");
        } catch (Exception e) {
            log.error("布隆过滤器初始化失败", e);
        }
    }

    /**
     * 定时更新布隆过滤器（可选）
     * 每天凌晨3点执行一次，确保布隆过滤器数据是最新的
     */
    @Scheduled(cron = "0 0 3 * * ?")
    public void refreshBloomFilter() {
        log.info("开始定时刷新布隆过滤器...");
        try {
            shopService.loadAllShopIdToBloomFilter();
            log.info("布隆过滤器刷新完成");
        } catch (Exception e) {
            log.error("布隆过滤器刷新失败", e);
        }
    }
    // 删除以下方法
    /**
     * 后台任务，将所有店铺id加载到布隆过滤器中
     */
    @Scheduled(fixedDelay = Long.MAX_VALUE) // 仅启动时执行一次
    public void loadAllShopIdToBloomFilter() {
        shopService.loadAllShopIdToBloomFilter();
    }
}