package com.seedoilz.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.seedoilz.gmall.realtime.common.base.BaseApp;
import com.seedoilz.gmall.realtime.common.bean.UserLoginBean;
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

public class DwsUserUserLoginWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsUserUserLoginWindow().start(10024, 4, "dws_user_user_login_window", Constant.TOPIC_DWD_TRAFFIC_PAGE);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 1. 读取Kafka页面主题数据
//        stream.print();

        // 2.将String转化为JSONObject，并且过滤数据
        SingleOutputStreamOperator<JSONObject> jsonObjectStream = etl(stream);

        // 3. 设置水位线
        SingleOutputStreamOperator<JSONObject> withWatermarkStream = getWithWatermarkStream(jsonObjectStream);

        // 4. 按照uid进行分组
        SingleOutputStreamOperator<UserLoginBean> keyedStream = mapUserLoginBeanStream(withWatermarkStream);

        // 5. 开窗聚合
        SingleOutputStreamOperator<UserLoginBean> windowedStream = windowAndAgg(keyedStream);

        // 6. 写入doris中
        windowedStream.map(new DorisMapFunction<>()).sinkTo(FlinkSinkUtil.getDorisSink(Constant.DWS_USER_USER_LOGIN_WINDOW));
    }

    private SingleOutputStreamOperator<UserLoginBean> windowAndAgg(SingleOutputStreamOperator<UserLoginBean> keyedStream) {
        SingleOutputStreamOperator<UserLoginBean> windowedStream = keyedStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(10L)))
                .reduce(new ReduceFunction<UserLoginBean>() {
                    @Override
                    public UserLoginBean reduce(UserLoginBean userLoginBean, UserLoginBean t1) throws Exception {
                        userLoginBean.setUuCt(userLoginBean.getUuCt() + t1.getUuCt());
                        userLoginBean.setBackCt(userLoginBean.getBackCt() + t1.getBackCt());
                        return userLoginBean;
                    }
                }, new ProcessAllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>.Context context, Iterable<UserLoginBean> iterable, Collector<UserLoginBean> collector) throws Exception {
                        TimeWindow window = context.window();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDt = DateFormatUtil.tsToDateTime(System.currentTimeMillis());
                        for (UserLoginBean userLoginBean : iterable) {
                            userLoginBean.setStt(stt);
                            userLoginBean.setEdt(edt);
                            userLoginBean.setCurDate(curDt);
                            collector.collect(userLoginBean);
                        }
                    }
                });
        return windowedStream;
    }

    private SingleOutputStreamOperator<UserLoginBean> mapUserLoginBeanStream(SingleOutputStreamOperator<JSONObject> withWatermarkStream) {
        SingleOutputStreamOperator<UserLoginBean> keyedStream = withWatermarkStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("common").getString("uid");
            }
        }).process(new KeyedProcessFunction<String, JSONObject, UserLoginBean>() {
            ValueState<String> lastLoginDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> lastLogintDtDesc = new ValueStateDescriptor<>("last_login_dt", String.class);
                lastLoginDtState = getRuntimeContext().getState(lastLogintDtDesc);
            }

            @Override
            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, UserLoginBean>.Context context, Collector<UserLoginBean> collector) throws Exception {
                String lastLoginDt = lastLoginDtState.value();
                Long ts = jsonObject.getLong("ts");
                String curDt = DateFormatUtil.tsToDate(ts);
                // 回流用户数
                Long backCt = 0L;
                // 独立用户数
                Long uuCt = 0L;

                if (lastLoginDt == null) {
                    uuCt = 1L;
                    lastLoginDtState.update(curDt);
                } else if (!lastLoginDt.equals(curDt)) {
                    uuCt = 1L;
                    lastLoginDtState.update(curDt);
                } else if (ts - DateFormatUtil.dateToTs(lastLoginDt) > 7 * 24 * 60 * 60 * 1000L) {
                    backCt = 1L;
                    lastLoginDtState.update(curDt);
                }

                // 不是独立用户肯定不是回流用户  不需要下游统计
                if (uuCt != 0) {
                    collector.collect(new UserLoginBean("", "", "", backCt, uuCt, ts));
                }
            }
        });
        return keyedStream;
    }

    private SingleOutputStreamOperator<JSONObject> getWithWatermarkStream(SingleOutputStreamOperator<JSONObject> jsonObjectStream) {
        return jsonObjectStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5L)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject jsonObject, long l) {
                return jsonObject.getLong("ts");
            }
        }));
    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        SingleOutputStreamOperator<JSONObject> jsonObjectStream = stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(s);
                JSONObject common = jsonObject.getJSONObject("common");
                String uid = common.getString("uid");
                if (uid != null) {
                    JSONObject page = jsonObject.getJSONObject("page");
                    String lastPageId = page.getString("last_page_id");
                    if (lastPageId == null || "login".equals(lastPageId)) {
                        collector.collect(jsonObject);
                    }
                }
            }
        });
        return jsonObjectStream;
    }
}
