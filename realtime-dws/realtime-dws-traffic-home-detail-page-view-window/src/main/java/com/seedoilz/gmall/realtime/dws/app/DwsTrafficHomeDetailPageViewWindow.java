package com.seedoilz.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.seedoilz.gmall.realtime.common.base.BaseApp;
import com.seedoilz.gmall.realtime.common.bean.TrafficHomeDetailPageViewBean;
import com.seedoilz.gmall.realtime.common.constant.Constant;
import com.seedoilz.gmall.realtime.common.function.DorisMapFunction;
import com.seedoilz.gmall.realtime.common.util.DateFormatUtil;
import com.seedoilz.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsTrafficHomeDetailPageViewWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTrafficHomeDetailPageViewWindow().start(10023, 4 , "dws_traffic_home_detail_page_view_window", Constant.TOPIC_DWD_TRAFFIC_PAGE);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 1. 读取DWD层page主题
//        stream.print();

        // 2. 清洗过滤数据
        SingleOutputStreamOperator<JSONObject> jsonObjStream = etl(stream);

        // 3. 设置水位线
        SingleOutputStreamOperator<JSONObject> withWatermarkStream = getWithWatermarkStream(jsonObjStream);

        // 4. 改变数据结构
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> beanStream = mapBeanStream(withWatermarkStream);

        // 5. reduce聚合窗口
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> reducedStream = reduceWindowStream(beanStream);

        // 6. 写入doris中
        reducedStream.map(new DorisMapFunction<>()).sinkTo(FlinkSinkUtil.getDorisSink(Constant.DWS_TRAFFIC_HOME_DETAIL_PAGE_VIEW_WINDOW));
    }

    private SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> reduceWindowStream(SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> beanStream) {
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> reducedStream = beanStream.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)))
                .reduce(new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                    @Override
                    public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean trafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean t1) throws Exception {
                        trafficHomeDetailPageViewBean.setGoodDetailUvCt(trafficHomeDetailPageViewBean.getGoodDetailUvCt() + t1.getGoodDetailUvCt());
                        trafficHomeDetailPageViewBean.setHomeUvCt(trafficHomeDetailPageViewBean.getHomeUvCt() + t1.getHomeUvCt());
                        return trafficHomeDetailPageViewBean;
                    }
                }, new AllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<TrafficHomeDetailPageViewBean> iterable, Collector<TrafficHomeDetailPageViewBean> collector) throws Exception {
                        String stt = DateFormatUtil.tsToDateTime(timeWindow.getStart());
                        String edt = DateFormatUtil.tsToDateTime(timeWindow.getEnd());
                        String curDt = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());
                        for (TrafficHomeDetailPageViewBean value : iterable) {
                            value.setStt(stt);
                            value.setEdt(edt);
                            value.setCurDate(curDt);
                            collector.collect(value);
                        }
                    }
                });
        return reducedStream;
    }

    private SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> mapBeanStream(SingleOutputStreamOperator<JSONObject> withWatermarkStream) {
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> beanStream = withWatermarkStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("common").getString("mid");
            }
        }).process(new KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>() {
            ValueState<String> homeLastLoginState;
            ValueState<String> detailLastLoginState;

            @Override
            public void open(Configuration parameters) {
                ValueStateDescriptor<String> homeLastLoginDesc = new ValueStateDescriptor<>("home_last_login", String.class);
                homeLastLoginDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1L)).build());
                homeLastLoginState = getRuntimeContext().getState(homeLastLoginDesc);

                ValueStateDescriptor<String> detailLastLoginDesc = new ValueStateDescriptor<>("detail_last_login", String.class);
                detailLastLoginDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1L)).build());
                detailLastLoginState = getRuntimeContext().getState(detailLastLoginDesc);
            }

            @Override
            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>.Context context, Collector<TrafficHomeDetailPageViewBean> collector) throws Exception {
                String pageId = jsonObject.getJSONObject("page").getString("page_id");
                Long homeUvCt = 0L;
                Long detailUvCt = 0L;

                Long ts = jsonObject.getLong("ts");
                String curDt = DateFormatUtil.tsToDate(ts);
                if ("home".equals(pageId)) {
                    String homeLastLoginDate = homeLastLoginState.value();
                    if (homeLastLoginDate == null || !homeLastLoginDate.equals(curDt)) {
                        homeUvCt = 1L;
                    }
                }
                if ("good_detail".equals(pageId)) {
                    String detailLastLoginDate = detailLastLoginState.value();
                    if (detailLastLoginDate == null || !detailLastLoginDate.equals(curDt)) {
                        detailUvCt = 1L;
                    }
                }
                if (homeUvCt != 0L || detailUvCt != 0L) {
                    collector.collect(TrafficHomeDetailPageViewBean.builder()
                            .homeUvCt(homeUvCt)
                            .goodDetailUvCt(detailUvCt)
                            .ts(ts)
                            .curDate(curDt)
                            .build());
                }
            }
        });
        return beanStream;
    }

    private SingleOutputStreamOperator<JSONObject> getWithWatermarkStream(SingleOutputStreamOperator<JSONObject> jsonObjStream) {
        SingleOutputStreamOperator<JSONObject> withWatermarkStream = jsonObjStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5L)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject jsonObject, long l) {
                return jsonObject.getLong("ts");
            }
        }));
        return withWatermarkStream;
    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        SingleOutputStreamOperator<JSONObject> jsonObjStream = stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(s);
                JSONObject page = jsonObject.getJSONObject("page");
                String pageId = page.getString("page_id");
                if (pageId != null) {
                    if ("home".equals(pageId) || "good_detail".equals(pageId)) {
                        collector.collect(jsonObject);
                    }
                }
            }
        });
        return jsonObjStream;
    }
}
