package com.hmdp.service.impl;

import cn.hutool.json.JSONUtil;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;

/**
 * <p>
 *  服务实现类
 * </p>
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private IShopTypeService typeService;

    @Override
    public List<ShopType> queryList() {
        //1. 查询redis
        String listStr = stringRedisTemplate.opsForValue().get("list");
        if(listStr != null){
            //2.如果有数据，返回
            List<ShopType> list = JSONUtil.toList(listStr,ShopType.class);
            return list;
        }
        //3.没有数据，查询数据库
        List<ShopType> list = typeService.query().orderByAsc("sort").list();
        //把数据写入redis
        stringRedisTemplate.opsForValue().set("list",JSONUtil.toJsonStr(list));
        //返回
        return list;
    }
}
