package com.seedoilz.gmall.realtime.dwd.db.split.app;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.seedoilz.gmall.realtime.common.base.BaseApp;
import com.seedoilz.gmall.realtime.common.constant.Constant;
import com.seedoilz.gmall.realtime.common.util.DateFormatUtil;
import com.seedoilz.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class DwdBaseLog extends BaseApp {
    public static void main(String[] args) {
        new DwdBaseLog().start(10011, 4, "DwdBaseLog", Constant.TOPIC_LOG);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 核心逻辑
        // 1. 进行ETL过滤不完整的数据
        SingleOutputStreamOperator<JSONObject> jsonObjStream = etl(stream);

        // 2. 新旧访客修复
        KeyedStream<JSONObject, String> keyedStream = keyByWithWaterMark(jsonObjStream);

        SingleOutputStreamOperator<JSONObject> isNewFixedStream = isNewFix(keyedStream);

//        isNewFixedStream.print();


        // 3. 拆分不同类型的用户行为日志
//        OutputTag<String> startTag = new OutputTag<String>("start"){};
        OutputTag<String> startTag = new OutputTag<String>("start", TypeInformation.of(String.class));
        OutputTag<String> errTag = new OutputTag<String>("err", TypeInformation.of(String.class));
        OutputTag<String> displayTag = new OutputTag<String>("display", TypeInformation.of(String.class));
        OutputTag<String> actionTag = new OutputTag<String>("action", TypeInformation.of(String.class));

        SingleOutputStreamOperator<String> pageStream = splitLog(isNewFixedStream, errTag, startTag, displayTag, actionTag);

        SideOutputDataStream<String> startStream = pageStream.getSideOutput(startTag);
        SideOutputDataStream<String> errStream = pageStream.getSideOutput(errTag);
        SideOutputDataStream<String> displayStream = pageStream.getSideOutput(displayTag);
        SideOutputDataStream<String> actionStream = pageStream.getSideOutput(actionTag);

        pageStream.print("page");
        startStream.print("start");
        errStream.print("err");
        displayStream.print("display");
        actionStream.print("action");

        pageStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
        startStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
        errStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
        displayStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
        actionStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));
    }

    /**
     * 将流进行拆分，并且将信息进行整合再输出
     * @param isNewFixedStream
     * @param errTag
     * @param startTag
     * @param displayTag
     * @param actionTag
     * @return
     */
    private SingleOutputStreamOperator<String> splitLog(SingleOutputStreamOperator<JSONObject> isNewFixedStream, OutputTag<String> errTag, OutputTag<String> startTag, OutputTag<String> displayTag, OutputTag<String> actionTag) {
        return isNewFixedStream.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) throws Exception {
                // 根据数据的不同，拆分到不同的输出流中
                JSONObject err = jsonObject.getJSONObject("err");
                if (err != null) {
                    context.output(errTag, err.toJSONString());
                    jsonObject.remove("err");
                }

                JSONObject page = jsonObject.getJSONObject("page");
                JSONObject start = jsonObject.getJSONObject("start");
                JSONObject common = jsonObject.getJSONObject("common");
                Long ts = jsonObject.getLong("ts");
                if (start != null) {
                    // 当前是启动日志
                    // 输出的是完整的信息
                    context.output(startTag, jsonObject.toJSONString());
                } else if (page != null) {
                    // 当前是页面日志
                    JSONArray displays = jsonObject.getJSONArray("displays");
                    if (displays != null) {
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            display.put("common", common);
                            display.put("ts", ts);
                            display.put("page", page);
                            context.output(displayTag, display.toJSONString());
                        }
                        jsonObject.remove("displays");
                    }

                    JSONArray actions = jsonObject.getJSONArray("actions");
                    if (actions != null) {
                        for (int j = 0; j < actions.size(); j++) {
                            JSONObject action = actions.getJSONObject(j);
                            action.put("common", common);
                            action.put("ts", ts);
                            action.put("page", page);
                            context.output(actionTag, actions.toJSONString());
                        }
                        jsonObject.remove("actions");
                    }


                    // 只保留page信息，写出到主流
                    collector.collect(jsonObject.toJSONString());
                } else {
                    // 留空
                }
            }
        });
    }

    /**
     * 对新用户的日志信息进行修正，有些老用户通过伪造信息来伪装新用户
     * 每一个用户都会维护一个第一次登录日期的状态，来进行检查
     * @param keyedStream
     * @return
     */
    public SingleOutputStreamOperator<JSONObject> isNewFix(KeyedStream<JSONObject, String> keyedStream) {
        return keyedStream.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
            ValueState<String> firstLoginDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 创建状态
                firstLoginDtState = getRuntimeContext().getState(new ValueStateDescriptor<String>("first_login_dt", String.class));
            }

            @Override
            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                // 1. 获取当前数据的is_new字段
                JSONObject common = jsonObject.getJSONObject("common");
                String isNew = common.getString("is_new");
                String firstLoginDt = firstLoginDtState.value();
                Long ts = jsonObject.getLong("ts");
                String curDt = DateFormatUtil.tsToDate(ts);
                if ("1".equals(isNew)) {
                    if (firstLoginDt != null && !firstLoginDt.equals(curDt)) {
                        // 如果状态不为空，且首次访问日期是今日，说明是老访客
                        common.put("is_new", "0");
                    } else if (firstLoginDt == null) {
                        // 状态为空
                        firstLoginDtState.update(curDt);
                    } else {
                        // 留空
                    }
                } else if ("0".equals(isNew)) {
                    if (firstLoginDt == null) {
                        // 老用户
                        // 一个好久没有上线的老用户，补充为昨天第一次登录
                        firstLoginDtState.update(DateFormatUtil.tsToDate(ts - 24 * 60 * 60 * 1000L));
                    } else {
                        // 正常情况
                        // 留空
                    }
                } else {
                    // 当前isNew字段有误
                }
                collector.collect(jsonObject);
            }
        });
    }

    /**
     * 对数据添加水位线（watermark）
     * 并且使用KeyBy对mid（每一个用户的唯一标识名）进行分区
     * @param jsonObjStream
     * @return
     */
    private KeyedStream<JSONObject, String> keyByWithWaterMark(SingleOutputStreamOperator<JSONObject> jsonObjStream) {
//        return jsonObjStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3L)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
//            @Override
//            public long extractTimestamp(JSONObject jsonObject, long l) {
//                return jsonObject.getLong("ts");
//            }
//        })).keyBy(new KeySelector<JSONObject, String>() {
//            @Override
//            public String getKey(JSONObject jsonObject) throws Exception {
//                return jsonObject.getJSONObject("common").getString("mid");
//            }
//        });

        return jsonObjStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject jsonObject, long l) {
                return jsonObject.getLong("ts");
            }
        })).keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("common").getString("mid");
            }
        });
    }

    /**
     * 做ETL，过滤脏数据，比如page为空或者start字段为空
     * @param stream
     * @return
     */
    public SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    JSONObject page = jsonObject.getJSONObject("page");
                    JSONObject start = jsonObject.getJSONObject("start");
                    JSONObject common = jsonObject.getJSONObject("common");
                    Long ts = jsonObject.getLong("ts");
                    if (page != null || start != null) {
                        if (common != null && common.getString("mid") != null && ts != null) {
                            collector.collect(jsonObject);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("过滤掉脏数据" + s);
                }
            }
        });
    }
}
