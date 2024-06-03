package com.seedoilz.gmall.realtime.common.function;

import com.alibaba.fastjson.JSONObject;
import com.seedoilz.gmall.realtime.common.bean.TradeSkuOrderBean;
import com.seedoilz.gmall.realtime.common.constant.Constant;
import com.seedoilz.gmall.realtime.common.util.HBaseUtil;
import com.seedoilz.gmall.realtime.common.util.RedisUtil;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.client.AsyncConnection;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimJoinFunction<T> {
    StatefulRedisConnection<String, String> redisAsyncConnection;
    AsyncConnection hBaseAsyncConnection;

    @Override
    public void close() throws Exception {
        RedisUtil.closeRedisAsyncConnection(redisAsyncConnection);
        HBaseUtil.closeAsyncConnection(hBaseAsyncConnection);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        redisAsyncConnection = RedisUtil.getRedisAsyncConnection();
        hBaseAsyncConnection = HBaseUtil.getHBaseAsyncConnection();
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        String rowKey = getId(input);
        String tableName = getTableName();
        String redisKey = RedisUtil.getRedisKey(tableName, rowKey);
        // java的异步编程方式
        CompletableFuture.supplyAsync(new Supplier<String>() {
            @Override
            public String get() {
                // 第一步 异步访问得到的数据
                RedisFuture<String> dimSkuInfoFuture = redisAsyncConnection.async().get(redisKey);
                String dimInfo = null;
                try {
                    dimInfo = dimSkuInfoFuture.get();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return dimInfo;
            }
        }).thenApplyAsync(new Function<String, JSONObject>() {
            JSONObject dimJsonObj;

            @Override
            public JSONObject apply(String dimInfo) {
                if (dimInfo == null || dimInfo.isEmpty()) {
                    try {
                        dimJsonObj = HBaseUtil.getAsyncCells(hBaseAsyncConnection, Constant.HBASE_NAMESPACE, tableName, rowKey);
                        // 将读取的数据保存到redis
                        redisAsyncConnection.async().setex(redisKey, 24 * 60 * 60, dimJsonObj.toJSONString());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    // 存在缓存数据
                    dimJsonObj = JSONObject.parseObject(dimInfo);
                }
                return dimJsonObj;
            }
        }).thenAccept(new Consumer<JSONObject>() {
            @Override
            public void accept(JSONObject dim) {
                // 合并维度信息
                if (dim == null) {
                    // 无法关联到维度信息
                    System.out.println("无法关联当前的维度信息" + tableName + ":" + rowKey);
                } else {
                    join(input, dim);
                }
                // 返回结果
                resultFuture.complete(Collections.singletonList(input));
            }
        });
    }
}
