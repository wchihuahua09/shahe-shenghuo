package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.yaml.snakeyaml.events.Event;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

@Slf4j
//记录日志
@Component
//这个注解，指bean交给spring来维护
public class CacheClient {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    public void set(String key, Object value, Long time, TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(value),time,unit);
    }
    public void setWithLogicExpire(String key, Object value, Long time, TimeUnit unit){
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(redisData));
    }

    public <R,ID> R queryWithPassThrough(
            String keyPrefix, ID id, Class<R> type, Function<ID,R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;

        //1.从redis中查询缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        //2.判断是否存在
        if(StrUtil.isNotBlank(json)){
            //3.存在，返回
            return JSONUtil.toBean(json, type);
        }
        //判断是否是空值
        if (json != null) {
            //是空字符
            return null;
        }
        //4.不存在，查询数据库
        R r = dbFallback.apply(id);
        //5.不存在，返回错误
        //将空值写入redis
        if(r == null){
            stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL,TimeUnit.MINUTES);
            return null;
        }
        //6.存在，将数据写入redis
        this.set(key,r,time,unit);
        //7.返回
        return r;
    }


    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
    private <R,ID> R queryWithLogicalExpire(
            String keyPrefix, ID id, Class<R> type, Function<ID,R> dbFallback, Long time, TimeUnit unit) {
        String key = CACHE_SHOP_KEY + id;

        //1.从redis中查询缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //2.判断是否存在
        if(StrUtil.isBlank(shopJson)){
            //3.不存在，返回null
            return null;
        }
        //4.存在，判断是否过期，把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        LocalDateTime expireTime = redisData.getExpireTime();
        if(expireTime.isAfter(LocalDateTime.now())){
            //5.未过期，返回用户信息
            return r;
        }
        //6.已过期，需要缓存重建
        String lockKey = LOCK_SHOP_KEY + id;
        //6.1尝试获取互斥锁
        boolean getLock = tryLock(lockKey);
        //6.2判断是否成功
        if(getLock){
            //成功，开启独立线程，实现缓存重建（利用线程池）
            try {
                CACHE_REBUILD_EXECUTOR.submit(() ->{
                    //缓存重建
                    R r1 = dbFallback.apply(id);
                    this.setWithLogicExpire(key,r1,time,unit);
                });
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                //释放锁
                unlock(lockKey);
            }
        }
        //7.返回
        return r;
    }

    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        //可能出现拆箱失败，所以用了这个
        return BooleanUtil.isTrue(flag);
    }

    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }
}
