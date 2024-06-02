package com.seedoilz.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.seedoilz.gmall.realtime.common.base.BaseApp;
import com.seedoilz.gmall.realtime.common.bean.TradeProvinceOrderBean;
import com.seedoilz.gmall.realtime.common.constant.Constant;
import com.seedoilz.gmall.realtime.common.function.DimAsyncFunction;
import com.seedoilz.gmall.realtime.common.function.DorisMapFunction;
import com.seedoilz.gmall.realtime.common.util.DateFormatUtil;
import com.seedoilz.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

public class DwsTradeProvinceOrderWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeProvinceOrderWindow().start(10020, 4, "dws_trade_province_order_window", Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 1. 读取主题数据
//        stream.print();

        // 2. 过滤null，转换数据结构
        SingleOutputStreamOperator<JSONObject> jsonObjStream = stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    if (s != null) {
                        JSONObject jsonObject = JSONObject.parseObject(s);
                        String id = jsonObject.getString("id");
                        String orderId = jsonObject.getString("order_id");
                        String provinceId = jsonObject.getString("province_id");
                        Long ts = jsonObject.getLong("ts");
                        if (id != null && orderId != null && provinceId != null && ts != null) {
                            jsonObject.put("ts", ts * 1000);
                            collector.collect(jsonObject);
                        }
                    }
                } catch (Exception e) {
                    System.out.println("过滤掉脏数据" + s);
                }
            }
        });

        // 3. 添加水位线
        SingleOutputStreamOperator<JSONObject> withWatermarkStream = jsonObjStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject jsonObject, long l) {
                return jsonObject.getLong("ts");
            }
        }));

        // 4. 按照唯一键去重，转换数据结构
        KeyedStream<JSONObject, String> keyedStream = withWatermarkStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getString("id");
            }
        });

        SingleOutputStreamOperator<TradeProvinceOrderBean> beanStream = keyedStream.map(new RichMapFunction<JSONObject, TradeProvinceOrderBean>() {
            ValueState<BigDecimal> lastTotalAmountState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<BigDecimal> lastTotalAmountDesc = new ValueStateDescriptor<>("last_total_amount", BigDecimal.class);
                lastTotalAmountDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(30L)).build());
                lastTotalAmountState = getRuntimeContext().getState(lastTotalAmountDesc);
            }

            @Override
            public TradeProvinceOrderBean map(JSONObject jsonObject) throws Exception {
                HashSet<String> hashSet = new HashSet<>();
                hashSet.add(jsonObject.getString("order_id"));
                BigDecimal lastTotalAmount = lastTotalAmountState.value();
                lastTotalAmount = lastTotalAmount == null ? new BigDecimal("0") : lastTotalAmount;
                BigDecimal splitTotalAmount = jsonObject.getBigDecimal("split_total_amount");
                lastTotalAmountState.update(splitTotalAmount);

                return TradeProvinceOrderBean.builder()
                        .orderIdSet(hashSet)
                        .provinceId(jsonObject.getString("province_id"))
                        .orderDetailId(jsonObject.getString("id"))
                        .ts(jsonObject.getLong("ts"))
                        .orderAmount(splitTotalAmount.subtract(lastTotalAmount))
                        .build();
            }
        });

//        beanStream.print();

        // 5. 按照省份ID分组
        SingleOutputStreamOperator<TradeProvinceOrderBean> reducedStream = beanStream.keyBy(new KeySelector<TradeProvinceOrderBean, String>() {
                    @Override
                    public String getKey(TradeProvinceOrderBean tradeProvinceOrderBean) throws Exception {
                        return tradeProvinceOrderBean.getProvinceId();
                    }
                }).window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)))
                .reduce(new ReduceFunction<TradeProvinceOrderBean>() {
                    @Override
                    public TradeProvinceOrderBean reduce(TradeProvinceOrderBean tradeProvinceOrderBean, TradeProvinceOrderBean t1) throws Exception {
                        tradeProvinceOrderBean.setOrderAmount(tradeProvinceOrderBean.getOrderAmount().add(t1.getOrderAmount()));
                        tradeProvinceOrderBean.getOrderIdSet().addAll(t1.getOrderIdSet());
                        return tradeProvinceOrderBean;
                    }
                }, new ProcessWindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>.Context context, Iterable<TradeProvinceOrderBean> iterable, Collector<TradeProvinceOrderBean> collector) throws Exception {
                        TimeWindow window = context.window();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDt = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());
                        for (TradeProvinceOrderBean element : iterable) {
                            element.setStt(stt);
                            element.setEdt(edt);
                            element.setCurDate(curDt);
                            element.setOrderCount((long) element.getOrderIdSet().size());
                            collector.collect(element);
                        }
                    }
                });

        SingleOutputStreamOperator<TradeProvinceOrderBean> provinceNameStream = AsyncDataStream.unorderedWait(reducedStream, new DimAsyncFunction<TradeProvinceOrderBean>() {
            @Override
            public String getId(TradeProvinceOrderBean input) {
                return input.getProvinceId();
            }

            @Override
            public String getTableName() {
                return "dim_base_province";
            }

            @Override
            public void join(TradeProvinceOrderBean input, JSONObject dim) {
                input.setProvinceName(dim.getString("name"));
            }
        }, 60, TimeUnit.SECONDS);

//        provinceNameStream.print();
        // 6. 写出到Doris中
        provinceNameStream.map(new DorisMapFunction<>()).sinkTo(FlinkSinkUtil.getDorisSink(Constant.DWS_TRADE_PROVINCE_ORDER_WINDOW));
    }
}
