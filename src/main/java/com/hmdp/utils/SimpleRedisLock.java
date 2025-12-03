package com.hmdp.utils;

import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.BooleanUtil;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class SimpleRedisLock implements ILock {

    public static final String KEY_PREFIX = "lock:";

    public static final DefaultRedisScript<Long> UNLOCK_SCRIPT;

    private String name;

    private StringRedisTemplate stringRedisTemplate;

    private String ID_PREFIX;

    static {
        UNLOCK_SCRIPT = new DefaultRedisScript<>();
        UNLOCK_SCRIPT.setLocation(new ClassPathResource("unlock.lua"));
        UNLOCK_SCRIPT.setResultType(Long.class);
    }

    public SimpleRedisLock(String name, StringRedisTemplate stringRedisTemplate) {
        this.name = name;
        this.stringRedisTemplate = stringRedisTemplate;
        this.ID_PREFIX = UUID.randomUUID().toString(true) + "-";
    }

    @Override
    public boolean tryLock(long timeoutSec) {
        String threadId = getThreadId();
        // 获取锁
        Boolean success = stringRedisTemplate.opsForValue().setIfAbsent(KEY_PREFIX + name, threadId, timeoutSec, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(success);
    }

    @Override
    public void unlock() {
        String threadId = getThreadId();
        // 调用lua脚本，保证get+del的原子性
        stringRedisTemplate.execute(UNLOCK_SCRIPT,
                Collections.singletonList(KEY_PREFIX + name),
                threadId);
    }

    private String getThreadId() {
        // 获取线程标识
        return ID_PREFIX + Thread.currentThread().getId();
    }
}
