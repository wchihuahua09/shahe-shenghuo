package com.hmdp.service.impl;

import com.baomidou.mybatisplus.core.toolkit.IdWorker;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
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
import java.util.concurrent.TimeUnit;

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
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedissonClient redissonClient;
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    //放在静态代码块中，类一加载就会一同加载
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    @Override
    public Result seckillVoucher(Long voucherId){
        Long id = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");
        // 1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), id.toString(),String.valueOf(orderId)
        );
        //2.判断是否是0
        int r = result.intValue();
        //2.1  没有
        if(r != 0){
            Result.fail(r == 1?"库存不足":"不能重复下单");
        }
        //2.2  0 有购买资格,保存阻塞队列

        // 3. 返回订单id；
        return Result.ok();
    }

    /*@Override

    public Result seckilVoucher(Long voucherId) throws InterruptedException {
        //1.查询优惠券
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        //2.判断秒杀是否开始
        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
            return Result.fail("秒杀没开始");
        }
        //3.判断秒杀是否结束
        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
            return Result.fail("秒杀已结束");
        }
        //4.判断库存是否充足
        if(voucher.getStock() < 1){
            return Result.fail("库存不足");
        }
        Long userId = UserHolder.getUser().getId();
//        SimpleRedisLock lock = new SimpleRedisLock("order:" + userId,stringRedisTemplate);
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        //失败了不等待
        boolean isLock = lock.tryLock(1L,TimeUnit.SECONDS);
        //判断是否获取锁成功
        if(!isLock){
            //获取锁失败，返回错误或重试
            return Result.fail("一个人只允许下一单");
        }

        try {
            //拿到事务代理对象，防止事务失效
            IVoucherOrderService proxy = (IVoucherOrderService)AopContext.currentProxy();
            return proxy.createVocherOrder(voucherId);
        } catch (IllegalStateException e) {
            throw new RuntimeException(e);
        } finally {
            //释放锁
            lock.unlock();
        }

    }*/

    @Override
    @Transactional
    public Result createVocherOrder(Long voucherId) {
        //6.一人一单
        //查询订单
        Long userId = UserHolder.getUser().getId();

        int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
        if(count > 0){
            //判断
            return Result.fail("已下过单");
        }
        //5.扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId).gt("stock",0)
                .update();
        if(!success){
            return Result.fail("库存不足");
        }
        //6.创建订单
        VoucherOrder voucherOrder = new VoucherOrder();
        //订单id，用工具类生成的
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        //用户id，从threadLocal里得到的
        voucherOrder.setUserId(userId);
        //代金券id，即前端传过来的id
        voucherOrder.setVoucherId(voucherId);
        save(voucherOrder);
        return Result.ok(orderId);
    }
}
