package com.seedoilz.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.seedoilz.gmall.realtime.common.base.BaseApp;
import com.seedoilz.gmall.realtime.common.bean.CartAddUuBean;
import com.seedoilz.gmall.realtime.common.constant.Constant;
import com.seedoilz.gmall.realtime.common.function.DorisMapFunction;
import com.seedoilz.gmall.realtime.common.util.DateFormatUtil;
import com.seedoilz.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsTradeCartAddUuWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeCartAddUuWindow().start(10026, 1, "dws_trade_cart_add_uu_window", Constant.TOPIC_DWD_TRADE_CART_ADD);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 1. 从Kafka加购明细主题读取数据
//        stream.print();

        // 2. 转换数据结构
        SingleOutputStreamOperator<JSONObject> beanStream = stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(s);
                String userId = jsonObject.getString("user_id");
                Long ts = jsonObject.getLong("ts");
                if (userId != null && ts != null) {
                    // 将时间戳修改为13位毫秒级
                    jsonObject.put("ts",ts * 1000L);
                    collector.collect(jsonObject);
                }
            }
        });

        // 3. 设置水位线
        SingleOutputStreamOperator<JSONObject> withWaterMarkStream = beanStream.assignTimestampsAndWatermarks(WatermarkStrategy
                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject jsonObject, long l) {
                        return jsonObject.getLong("ts");
                    }
                }));

        // 4. 按照用户id进行分组
        SingleOutputStreamOperator<CartAddUuBean> keyedStream = withWaterMarkStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getString("user_id");
            }
        }).process(new KeyedProcessFunction<String, JSONObject, CartAddUuBean>() {
            ValueState<String> lastCartAddDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> lastCartAddDtDesc = new ValueStateDescriptor<>("last_cart_add_dt", String.class);
                lastCartAddDtState = getRuntimeContext().getState(lastCartAddDtDesc);
            }

            @Override
            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, CartAddUuBean>.Context context, Collector<CartAddUuBean> collector) throws Exception {
                String lastCartAddDt = lastCartAddDtState.value();
                String curDt = DateFormatUtil.tsToDate(jsonObject.getLong("ts"));
                if (lastCartAddDt == null || !lastCartAddDt.equals(curDt)) {
                    lastCartAddDtState.update(curDt);
                    collector.collect(new CartAddUuBean("", "", curDt, 1L));
                }
            }
        });

        SingleOutputStreamOperator<CartAddUuBean> reducedStream = keyedStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(10L))).reduce(new ReduceFunction<CartAddUuBean>() {
            @Override
            public CartAddUuBean reduce(CartAddUuBean cartAddUuBean, CartAddUuBean t1) throws Exception {
                cartAddUuBean.setCartAddUuCt(cartAddUuBean.getCartAddUuCt() + t1.getCartAddUuCt());
                return cartAddUuBean;
            }
        }, new ProcessAllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>() {
            @Override
            public void process(ProcessAllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>.Context context, Iterable<CartAddUuBean> iterable, Collector<CartAddUuBean> collector) throws Exception {
                TimeWindow window = context.window();
                String stt = DateFormatUtil.tsToDateTime(window.getStart());
                String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                String curDt = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());
                for (CartAddUuBean cartAddUuBean : iterable) {
                    cartAddUuBean.setStt(stt);
                    cartAddUuBean.setEdt(edt);
                    cartAddUuBean.setCurDate(curDt);
                    collector.collect(cartAddUuBean);
                }
            }
        });

        reducedStream.map(new DorisMapFunction<>()).sinkTo(FlinkSinkUtil.getDorisSink(Constant.DWS_TRADE_CART_ADD_UU_WINDOW));
    }
}
