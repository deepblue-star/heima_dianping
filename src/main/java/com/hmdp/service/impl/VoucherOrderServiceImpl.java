package com.hmdp.service.impl;

import com.alibaba.fastjson.JSON;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.mq.dto.CreateOrderDTO;
import com.hmdp.mq.producer.MQProducerService;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.Collections;

import static com.hmdp.utils.MQConstants.SECKILL_VOUCHER_ORDER_TOPIC;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private RedissonClient redissonClient;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private MQProducerService mqProducerService;

//    @Override
//    public Result seckillVoucherSync(Long voucherId) {
//        // 1. 查优惠券
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//
//        if (voucher == null) {
//            // 优惠券不存在
//            return Result.fail("优惠券不存在！");
//        }
//
//        // 2. 判断秒杀是否开始
//        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
//            // 未开始
//            return Result.fail("秒杀未开始！");
//        }
//
//        // 3. 判断秒杀是否结束
//        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
//            // 未开始
//            return Result.fail("秒杀已结束！");
//        }
//
//        Long userId = UserHolder.getUser().getId();
//        RLock redisLock = redissonClient.getLock("order:" + userId);
//
//        // 尝试获取锁
//        boolean isLock = redisLock.tryLock();
//        if (!isLock) {
//            // 获取锁失败
//            return Result.fail("不能重复下单！");
//        }
//        try {
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId, voucher);
//        } finally {
//            redisLock.unlock();
//        }
//    }
//
//    @Transactional
//    public Result createVoucherOrder(Long voucherId, SeckillVoucher voucher) {
//        // 4. 判断用户是否重复下单
//        Long userId = UserHolder.getUser().getId();
//        int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
//        if (count > 0) {
//            // 用户已经购买过了
//            return Result.fail("用户已经购买过一次！");
//        }
//
//        // 5. 判断库存是否充足
//        if (voucher.getStock() < 1) {
//            // 库存不足
//            return Result.fail("库存不足！");
//        }
//
//        // 6. 扣减库存
//        boolean success = seckillVoucherService.update()
//                .setSql("stock = stock - 1")
//                .gt("stock",  0)
//                .eq("voucher_id", voucherId).update();
//        if (!success) {
//            return Result.fail("库存不足！");
//        }
//
//        // 6. 创建订单
//        VoucherOrder voucherOrder = new VoucherOrder();
//        // 6.1. 订单id
//        long orderId = redisIdWorker.nextId("order");
//        voucherOrder.setId(orderId);
//        // 6.2. 用户id
//        voucherOrder.setUserId(userId);
//        // 6.3. 代金券id
//        voucherOrder.setVoucherId(voucherId);
//        save(voucherOrder);
//
//        // 7. 返回订单id
//        return Result.ok(orderId);
//    }

    @Override
    public Result seckillVoucherAsync(Long voucherId) {
        // todo: 判断秒杀时间
        // 1. 执行lua脚本
        Long userId = UserHolder.getUser().getId();
        Long result = stringRedisTemplate.execute(SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString());

        // 2. 判断结果是否为0
        int r = result.intValue();
        // 2.1. 不为0，说明没有购买资格
        if (r != 0) {
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }

        // 2.2. 为0，有购买资格，把下单信息存到阻塞队列
        VoucherOrder voucherOrder = new VoucherOrder();
        // 2.3. 订单id
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        // 2.4. 用户id
        voucherOrder.setUserId(userId);
        // 2.5. 代金券id
        voucherOrder.setVoucherId(voucherId);

        // 2.6. 发送MQ消息
        CreateOrderDTO createOrderDTO = new CreateOrderDTO();
        createOrderDTO.setUserId(userId);
        createOrderDTO.setVoucherId(voucherId);
        createOrderDTO.setOrderId(orderId);
        mqProducerService.sendSeckillMsgSync(SECKILL_VOUCHER_ORDER_TOPIC, JSON.toJSONString(createOrderDTO));

        // 3. 返回订单id
        return Result.ok(orderId);
    }

    @Transactional
    @Override
    public boolean createVoucherOrder(Long userId, Long voucherId, Long orderId) {
        // 0. 先查后写，保证幂等，避免重复消费
        int count = query().eq("id", orderId).count();
        if (count > 0) {
            // 用户已经购买过了
            log.debug("订单id: " + orderId + "已存在，可能存在重复消费");
            return false;
        }

        // 1. 扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .gt("stock", 0)
                .eq("voucher_id", voucherId).update();
        if (!success) {
            return false;
        }
        // 2. 创建订单
        VoucherOrder voucherOrder = new VoucherOrder();
        // 2.1. 订单id
        voucherOrder.setId(orderId);
        // 6.2. 用户id
        voucherOrder.setUserId(userId);
        // 6.3. 代金券id
        voucherOrder.setVoucherId(voucherId);
        save(voucherOrder);

        // 7. 返回订单id
        return true;
    }
}
