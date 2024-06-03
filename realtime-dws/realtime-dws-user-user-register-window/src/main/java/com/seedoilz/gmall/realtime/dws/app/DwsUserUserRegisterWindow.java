package com.seedoilz.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.seedoilz.gmall.realtime.common.base.BaseApp;
import com.seedoilz.gmall.realtime.common.bean.UserRegisterBean;
import com.seedoilz.gmall.realtime.common.constant.Constant;
import com.seedoilz.gmall.realtime.common.function.DorisMapFunction;
import com.seedoilz.gmall.realtime.common.util.DateFormatUtil;
import com.seedoilz.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsUserUserRegisterWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsUserUserRegisterWindow().start(10025, 1, "realtime_dws_user_user_register_window", Constant.TOPIC_DWD_USER_REGISTER);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 1. 读取Kafka用户注册主题数据
//        stream.print();

        // 2. 转换数据结构
        SingleOutputStreamOperator<UserRegisterBean> beanStream = stream.flatMap(new FlatMapFunction<String, UserRegisterBean>() {
            @Override
            public void flatMap(String s, Collector<UserRegisterBean> collector) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(s);
                String createTime = jsonObject.getString("create_time");
                String id = jsonObject.getString("id");
                if (createTime != null && id != null) {
                    collector.collect(new UserRegisterBean("", "", "", 1L,createTime));
                }
            }
        });

        // 3. 设置水位线
        SingleOutputStreamOperator<UserRegisterBean> withWatermarkStream = beanStream.assignTimestampsAndWatermarks(WatermarkStrategy
                .<UserRegisterBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                .withTimestampAssigner(new SerializableTimestampAssigner<UserRegisterBean>() {
            @Override
            public long extractTimestamp(UserRegisterBean userRegisterBean, long l) {
                return DateFormatUtil.dateTimeToTs(userRegisterBean.getCreateTime());
            }
        }));

        // 4. 开窗聚合
        SingleOutputStreamOperator<UserRegisterBean> reducedStream = withWatermarkStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(5L)))
                .reduce(new ReduceFunction<UserRegisterBean>() {
                    @Override
                    public UserRegisterBean reduce(UserRegisterBean userRegisterBean, UserRegisterBean t1) throws Exception {
                        userRegisterBean.setRegisterCt(userRegisterBean.getRegisterCt() + t1.getRegisterCt());
                        return userRegisterBean;
                    }
                }, new ProcessAllWindowFunction<UserRegisterBean, UserRegisterBean, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<UserRegisterBean, UserRegisterBean, TimeWindow>.Context context, Iterable<UserRegisterBean> iterable, Collector<UserRegisterBean> collector) throws Exception {
                        TimeWindow window = context.window();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDt = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());
                        for (UserRegisterBean userRegisterBean : iterable) {
                            userRegisterBean.setStt(stt);
                            userRegisterBean.setEdt(edt);
                            userRegisterBean.setCurDate(curDt);
                            collector.collect(userRegisterBean);
                        }
                    }
                });

        reducedStream.map(new DorisMapFunction<>()).sinkTo(FlinkSinkUtil.getDorisSink(Constant.DWS_USER_USER_REGISTER_WINDOW));
    }
}
