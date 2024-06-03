package com.seedoilz.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.seedoilz.gmall.realtime.common.base.BaseApp;
import com.seedoilz.gmall.realtime.common.bean.TradeSkuOrderBean;
import com.seedoilz.gmall.realtime.common.constant.Constant;
import com.seedoilz.gmall.realtime.common.function.DorisMapFunction;
import com.seedoilz.gmall.realtime.common.util.DateFormatUtil;
import com.seedoilz.gmall.realtime.common.util.FlinkSinkUtil;
import com.seedoilz.gmall.realtime.common.util.HBaseUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.math.BigDecimal;
import java.time.Duration;

public class DwsTradeSkuOrderWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeSkuOrderWindow().start(10029, 4, "dws-trade-sku-order-window", Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 1. 读取DWD下单主题数据
//        stream.print();

        // 2. 过滤清洗null
        SingleOutputStreamOperator<JSONObject> jsonObjectStream = etl(stream);

        // 3. 添加水位线
        SingleOutputStreamOperator<JSONObject> withWatermarkStream = withWaterMark(jsonObjectStream);

        // 4. 修正度量值，转换数据结构
        KeyedStream<JSONObject, String> keyedStream = keyStream(withWatermarkStream);

        SingleOutputStreamOperator<TradeSkuOrderBean> processBeanStream = processBeanStream(keyedStream);
//        processBeanStream.print();

        // 5. 分组开窗聚合
        SingleOutputStreamOperator<TradeSkuOrderBean> reduceBeanStream = reduceBeanStream(processBeanStream);

//        reduceBeanStream.print();

        // 6. 关联维度信息
        // 6.1 关联sku_info，补充维度信息
        SingleOutputStreamOperator<TradeSkuOrderBean> fullDimStream = associateDim(reduceBeanStream);

//        fullDimStream.print();
        // 7. 写出到doris中
        fullDimStream.map(new DorisMapFunction<>()).sinkTo(FlinkSinkUtil.getDorisSink(Constant.DWS_TRADE_SKU_ORDER_WINDOW));
    }

    private SingleOutputStreamOperator<TradeSkuOrderBean> associateDim(SingleOutputStreamOperator<TradeSkuOrderBean> reduceBeanStream) {
        SingleOutputStreamOperator<TradeSkuOrderBean> fullDimStream = reduceBeanStream.map(new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
            Connection connection;

            @Override
            public void close() throws Exception {
                HBaseUtil.closeConnection(connection);
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                connection = HBaseUtil.getHBaseConnection();
            }

            @Override
            public TradeSkuOrderBean map(TradeSkuOrderBean tradeSkuOrderBean) throws Exception {
                // (1) 使用hbase api读取表格数据
                JSONObject dimSkuInfo = HBaseUtil.getCells(connection, Constant.HBASE_NAMESPACE, "dim_sku_info", tradeSkuOrderBean.getSkuId());
                // (2) 使用读取到的字段进行补充
                tradeSkuOrderBean.setCategory3Id(dimSkuInfo.getString("category3_id"));
                tradeSkuOrderBean.setTrademarkId(dimSkuInfo.getString("tm_id"));
                tradeSkuOrderBean.setSpuId(dimSkuInfo.getString("spu_id"));
                tradeSkuOrderBean.setSkuName(dimSkuInfo.getString("sku_name"));

                // (3) 继续关联别的维度表
                // 关联spu表格
                JSONObject dimSpuInfo = HBaseUtil.getCells(connection, Constant.HBASE_NAMESPACE, "dim_spu_info", tradeSkuOrderBean.getSpuId());
                tradeSkuOrderBean.setSpuName(dimSpuInfo.getString("spu_name"));

                // 关联c3表格
                JSONObject dimC3 = HBaseUtil.getCells(connection, Constant.HBASE_NAMESPACE, "dim_base_category3", tradeSkuOrderBean.getCategory3Id());
                tradeSkuOrderBean.setCategory3Name(dimC3.getString("name"));
                tradeSkuOrderBean.setCategory2Id(dimC3.getString("category2_id"));

                // 关联c2表格
                JSONObject dimC2 = HBaseUtil.getCells(connection, Constant.HBASE_NAMESPACE, "dim_base_category2", tradeSkuOrderBean.getCategory2Id());
                tradeSkuOrderBean.setCategory2Name(dimC2.getString("name"));
                tradeSkuOrderBean.setCategory1Id(dimC2.getString("category1_id"));

                // 关联c1表格
                JSONObject dimC1 = HBaseUtil.getCells(connection, Constant.HBASE_NAMESPACE, "dim_base_category1", tradeSkuOrderBean.getCategory1Id());
                tradeSkuOrderBean.setCategory1Name(dimC1.getString("name"));

                // 关联品牌表
                JSONObject dimTm = HBaseUtil.getCells(connection, Constant.HBASE_NAMESPACE, "dim_base_trademark", tradeSkuOrderBean.getTrademarkId());
                tradeSkuOrderBean.setTrademarkName(dimTm.getString("tm_name"));
                return tradeSkuOrderBean;
            }
        });
        return fullDimStream;
    }

    private SingleOutputStreamOperator<TradeSkuOrderBean> reduceBeanStream(SingleOutputStreamOperator<TradeSkuOrderBean> processBeanStream) {
        SingleOutputStreamOperator<TradeSkuOrderBean> reduceBeanStream = processBeanStream.keyBy(new KeySelector<TradeSkuOrderBean, String>() {
                    @Override
                    public String getKey(TradeSkuOrderBean tradeSkuOrderBean) throws Exception {
                        return tradeSkuOrderBean.getSkuId();
                    }
                }).window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)))
                .reduce(new ReduceFunction<TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean reduce(TradeSkuOrderBean tradeSkuOrderBean, TradeSkuOrderBean t1) throws Exception {
                        tradeSkuOrderBean.setOriginalAmount(t1.getOriginalAmount().add(tradeSkuOrderBean.getOriginalAmount()));
                        tradeSkuOrderBean.setCouponReduceAmount(t1.getCouponReduceAmount().add(tradeSkuOrderBean.getCouponReduceAmount()));
                        tradeSkuOrderBean.setActivityReduceAmount(t1.getActivityReduceAmount().add(tradeSkuOrderBean.getActivityReduceAmount()));
                        tradeSkuOrderBean.setOrderAmount(t1.getOrderAmount().add(tradeSkuOrderBean.getOrderAmount()));
                        return tradeSkuOrderBean;
                    }
                }, new ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>.Context context, Iterable<TradeSkuOrderBean> iterable, Collector<TradeSkuOrderBean> collector) throws Exception {
                        TimeWindow window = context.window();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDt = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());
                        for (TradeSkuOrderBean tradeSkuOrderBean : iterable) {
                            tradeSkuOrderBean.setStt(stt);
                            tradeSkuOrderBean.setEdt(edt);
                            tradeSkuOrderBean.setCurDate(curDt);
                            collector.collect(tradeSkuOrderBean);
                        }
                    }
                });
        return reduceBeanStream;
    }

    private SingleOutputStreamOperator<TradeSkuOrderBean> processBeanStream(KeyedStream<JSONObject, String> keyedStream) {
        SingleOutputStreamOperator<TradeSkuOrderBean> processBeanStream = keyedStream.process(new KeyedProcessFunction<String, JSONObject, TradeSkuOrderBean>() {
            MapState<String, BigDecimal> lastAmountState;

            @Override
            public void open(Configuration parameters) throws Exception {
                MapStateDescriptor<String, BigDecimal> lastAmountDesc = new MapStateDescriptor<>("last_amount", String.class, BigDecimal.class);
                lastAmountDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(30L)).build());
                lastAmountState = getRuntimeContext().getMapState(lastAmountDesc);
            }

            @Override
            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, TradeSkuOrderBean>.Context context, Collector<TradeSkuOrderBean> collector) throws Exception {
                BigDecimal originalAmount = lastAmountState.get("originalAmount");
                BigDecimal activityReduceAmount = lastAmountState.get("activityReduceAmount");
                BigDecimal couponReduceAmount = lastAmountState.get("couponReduceAmount");
                BigDecimal orderAmount = lastAmountState.get("orderAmount");

                originalAmount = originalAmount == null ? new BigDecimal("0") : originalAmount;
                activityReduceAmount = activityReduceAmount == null ? new BigDecimal("0") : activityReduceAmount;
                couponReduceAmount = couponReduceAmount == null ? new BigDecimal("0") : couponReduceAmount;
                orderAmount = orderAmount == null ? new BigDecimal("0") : orderAmount;

                BigDecimal curOriginalAmount = jsonObject.getBigDecimal("order_price").multiply(jsonObject.getBigDecimal("sku_num"));
                TradeSkuOrderBean bean = TradeSkuOrderBean.builder()
                        .skuId(jsonObject.getString("sku_id"))
                        .orderDetailId(jsonObject.getString("id"))
                        .ts(jsonObject.getLong("ts"))
                        .originalAmount(curOriginalAmount.subtract(originalAmount))
                        .orderAmount(jsonObject.getBigDecimal("split_total_amount").subtract(orderAmount))
                        .activityReduceAmount(jsonObject.getBigDecimal("split_activity_amount").subtract(activityReduceAmount))
                        .couponReduceAmount(jsonObject.getBigDecimal("split_coupon_amount").subtract(couponReduceAmount))
                        .build();

                // 存储当前的度量值
                lastAmountState.put("originalAmount", curOriginalAmount);
                lastAmountState.put("activityReduceAmount", jsonObject.getBigDecimal("split_activity_amount"));
                lastAmountState.put("couponReduceAmount", jsonObject.getBigDecimal("split_coupon_amount"));
                lastAmountState.put("orderAmount", jsonObject.getBigDecimal("split_total_amount"));
                collector.collect(bean);
            }
        });
        return processBeanStream;
    }

    private KeyedStream<JSONObject, String> keyStream(SingleOutputStreamOperator<JSONObject> withWatermarkStream) {
        KeyedStream<JSONObject, String> keyedStream = withWatermarkStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getString("id");
            }
        });
        return keyedStream;
    }

    private SingleOutputStreamOperator<JSONObject> withWaterMark(SingleOutputStreamOperator<JSONObject> jsonObjectStream) {
        SingleOutputStreamOperator<JSONObject> withWatermarkStream = jsonObjectStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5L)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject jsonObject, long l) {
                return jsonObject.getLong("ts");
            }
        }));
        return withWatermarkStream;
    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        SingleOutputStreamOperator<JSONObject> jsonObjectStream = stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    if (s != null) {
                        JSONObject jsonObject = JSONObject.parseObject(s);
                        Long ts = jsonObject.getLong("ts");
                        String id = jsonObject.getString("id");
                        String skuId = jsonObject.getString("sku_id");
                        if (ts != null && id != null && skuId != null) {
                            jsonObject.put("ts", ts * 1000);
                            collector.collect(jsonObject);
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException("过滤脏数据" + s);
                }
            }
        });
        return jsonObjectStream;
    }
}
