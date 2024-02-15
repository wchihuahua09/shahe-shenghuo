package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Component
public class RedisIdWorker {
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    private static final long BEGIN_TIMESTAMP = 1640995200L;
    private static final int COUNT_BITS = 32;
    public long nextId(String keyPrefix){
        //时间戳：1640995200
        //1.生成时间戳
        LocalDateTime localDateTime = LocalDateTime.now();
        long nowSecond = localDateTime.toEpochSecond(ZoneOffset.UTC);
        long timestamp = nowSecond - BEGIN_TIMESTAMP;
        //2.生成序列号
        //获取当前日期精确到天
        String date = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        Long count = stringRedisTemplate.opsForValue().increment("icr" + keyPrefix + ":" + date);
        //3.拼接并返回

        return timestamp << COUNT_BITS | count;
    }

    public static void main(String[] args) {
        LocalDateTime localDateTime = LocalDateTime.of(2022, 1, 1, 0, 0, 0);

        long epochSecond = localDateTime.toEpochSecond(ZoneOffset.UTC);
        System.out.println("时间戳：" + epochSecond);
    }
}
