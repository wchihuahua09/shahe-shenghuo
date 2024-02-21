package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * <p>
 *  服务实现类
 * </p>
 */
@Service
@Slf4j
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
    private ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();
    @PostConstruct
    private void init(){
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }


        private class VoucherOrderHandler implements Runnable{
        String queueName = "stream.orders";
        @Override
        public void run() {
            while(true){
                //1. 获取队列中的订单信息
                try {
                    //1.拿消息队列的订单信息 xreadgroup group g1 c1 count 1 block 2000 streams streams.order >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            //ReadOffset.lastConsumed()这个就是大于号
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    //2.判断消息是否成功
                    if(list == null||list.isEmpty()){
                        //2.1获取失败，没有消息，继续下一次
                        continue;
                    }
                    //3. 解析消息
                    MapRecord<String, Object, Object> entries = list.get(0);
                    Map<Object, Object> value = entries.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    //3.有消息，可以下单
                    handleVoucherOrder(voucherOrder);
                    //4.ack确认 sack stream.orders g1
                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",entries.getId());
                } catch (Exception e) {
                    handlePendingList();
                }
            }
        }

            private void handlePendingList() {
                while(true){
                    //1. 获取队列中的订单信息
                    try {
                        //1.拿pendinglist的订单信息 xreadgroup group g1 c1 count 1 block 2000 streams streams.order >
                        List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                                Consumer.from("g1", "c1"),
                                StreamReadOptions.empty().count(1),
                                //ReadOffset.lastConsumed()这个就是大于号
                                StreamOffset.create(queueName, ReadOffset.from("0"))
                        );
                        //2.判断消息是否成功
                        if(list == null||list.isEmpty()){
                            //2.1获取失败，没有消息，继续下一次
                            break;
                        }
                        //3. 解析消息
                        MapRecord<String, Object, Object> entries = list.get(0);
                        Map<Object, Object> value = entries.getValue();
                        VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                        //3.有消息，可以下单
                        handleVoucherOrder(voucherOrder);
                        //4.ack确认 sack stream.orders g1
                        stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",entries.getId());
                    } catch (Exception e) {
                        log.info("处理pending-list异常",e);
                        try {
                            Thread.sleep(20);
                        } catch (InterruptedException ex) {
                            throw new RuntimeException(ex);
                        }
                    }
                }
            }
        }
/*    private class VoucherOrderHandler implements Runnable{
        @Override
        public void run() {
            while(true){
                //1. 获取队列中的订单信息
                try {
                    VoucherOrder order = orderTasks.take();
                    handleVoucherOrder(order);
                } catch (Exception e) {
                    log.info("处理订单异常",e);
                }
            }
        }
    }*/


    private IVoucherOrderService proxy;

    @Override
    public Result seckillVoucher(Long voucherId){
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");
        // 1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(),String.valueOf(orderId)
        );
        //2.判断是否是0
        int r = result.intValue();
        //2.1  没有
        if(r != 0){
            return Result.fail(r == 1?"库存不足":"不能重复下单");
        }
        //获取代理对象
        proxy = (IVoucherOrderService)AopContext.currentProxy();
        // 3. 返回订单id；
        return Result.ok(orderId);
    }

    /*@Override
    public Result seckillVoucher(Long voucherId){
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");
        // 1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(),String.valueOf(orderId)
        );
        //2.判断是否是0
        int r = result.intValue();
        //2.1  没有
        if(r != 0){
            return Result.fail(r == 1?"库存不足":"不能重复下单");
        }
        //2.2  0 有购买资格,保存阻塞队列
        //6.创建订单
        VoucherOrder voucherOrder = new VoucherOrder();
        //订单id，用工具类生成的
        voucherOrder.setId(orderId);
        //用户id，从threadLocal里得到的
        voucherOrder.setUserId(userId);
        //代金券id，即前端传过来的id
        voucherOrder.setVoucherId(voucherId);
        orderTasks.add(voucherOrder);

        //获取代理对象
        proxy = (IVoucherOrderService)AopContext.currentProxy();
        // 3. 返回订单id；
        return Result.ok(orderId);
    }*/

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

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        //SimpleRedisLock lock = new SimpleRedisLock("order:" + userId,stringRedisTemplate);
        Long userId = voucherOrder.getUserId();
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        //失败了不等待
        boolean isLock = lock.tryLock();
        //判断是否获取锁成功
        if(!isLock){
            //获取锁失败，返回错误或重试
            log.error("不允许重复下单");
            return;
        }
        try {
            //拿到事务代理对象，防止事务失效
            proxy.createVocherOrder(voucherOrder);
        } catch (IllegalStateException e) {
            throw new RuntimeException(e);
        } finally {
            //释放锁
            lock.unlock();
        }
    }

    @Override
    @Transactional
    public void createVocherOrder(VoucherOrder voucherOrder) {
        //6.一人一单
        //查询订单
        Long userId = voucherOrder.getUserId();
        int count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
        if(count > 0){
            //判断
            log.error("用户已经购买一次");
            return;
        }
        //5.扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherOrder.getVoucherId()).gt("stock",0)
                .update();
        if(!success){
            log.error("库存不足");
            return ;
        }
        save(voucherOrder);
    }
}
