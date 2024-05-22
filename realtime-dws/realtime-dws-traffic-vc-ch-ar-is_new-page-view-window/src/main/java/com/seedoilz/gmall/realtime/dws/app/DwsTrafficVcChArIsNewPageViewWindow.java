package com.seedoilz.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.seedoilz.gmall.realtime.common.base.BaseApp;
import com.seedoilz.gmall.realtime.common.bean.TrafficPageViewBean;
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
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsTrafficVcChArIsNewPageViewWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTrafficVcChArIsNewPageViewWindow().start(10022, 4, "dws_traffic_vc_ch_ar_is_new_page_view_window", Constant.TOPIC_DWD_TRAFFIC_PAGE);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        //核心业务
        // 1. 读取DWD的page业务数据
//        stream.print();

        // 2. 清洗清洗过滤，同时转化结构为jsonObject
        SingleOutputStreamOperator<JSONObject> jsonObjStream = etl(stream);

        // 3. 按照mid进行分组  判断独立访客
        SingleOutputStreamOperator<TrafficPageViewBean> beanStream = mapUvBean(jsonObjStream);

//        beanStream.print();

        // 4. 添加水位线
        SingleOutputStreamOperator<TrafficPageViewBean> withWaterMarkStream = withWaterMark(beanStream);

        // 5. 按照粒度分组
        KeyedStream<TrafficPageViewBean, String> keyedStream = keyByVcChArIsnew(withWaterMarkStream);

        // 6. 开窗
        WindowedStream<TrafficPageViewBean, String, TimeWindow> windowStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10L)));

        // 7. 聚合
        SingleOutputStreamOperator<TrafficPageViewBean> reduceFullStream = getReduceFullStream(windowStream);

        // 8. 写出到doris
        reduceFullStream.map(new DorisMapFunction<TrafficPageViewBean>()
        ).sinkTo(FlinkSinkUtil.getDorisSink(Constant.DWS_TRAFFIC_VC_CH_AR_IS_NEW_PAGE_VIEW_WINDOW));

    }

    private SingleOutputStreamOperator<TrafficPageViewBean> getReduceFullStream(WindowedStream<TrafficPageViewBean, String, TimeWindow> windowStream) {
        return windowStream.reduce(new ReduceFunction<TrafficPageViewBean>() {
            @Override
            public TrafficPageViewBean reduce(TrafficPageViewBean t1, TrafficPageViewBean t2) throws Exception {
                // t1 表示累加的结果（第一次的话就是第一个元素，从第二个开始)
                // t2 新来的值
                t1.setSvCt(t1.getSvCt() + t2.getSvCt());
                t1.setUvCt(t1.getUvCt() + t2.getUvCt());
                t1.setPvCt(t1.getPvCt() + t2.getPvCt());
                t1.setDurSum(t1.getDurSum() + t2.getDurSum());
                return t1;
            }
        }, new ProcessWindowFunction<TrafficPageViewBean, TrafficPageViewBean, String, TimeWindow>() {
            @Override
            public void process(String s, ProcessWindowFunction<TrafficPageViewBean, TrafficPageViewBean, String, TimeWindow>.Context context, Iterable<TrafficPageViewBean> iterable, Collector<TrafficPageViewBean> collector) throws Exception {
                TimeWindow window = context.window();
                String stt = DateFormatUtil.tsToDate(window.getStart());
                String edt = DateFormatUtil.tsToDate(window.getEnd());
                String curDt = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());
                for (TrafficPageViewBean element : iterable) {
                    element.setStt(stt);
                    element.setEdt(edt);
                    element.setCur_date(curDt);
                    collector.collect(element);
                }
            }
        });
    }

    private KeyedStream<TrafficPageViewBean, String> keyByVcChArIsnew(SingleOutputStreamOperator<TrafficPageViewBean> withWaterMarkStream) {
        return withWaterMarkStream.keyBy(new KeySelector<TrafficPageViewBean, String>() {
            @Override
            public String getKey(TrafficPageViewBean trafficPageViewBean) throws Exception {
                return trafficPageViewBean.getVc() + ":" + trafficPageViewBean.getCh() + ":" + trafficPageViewBean.getAr() + ":" + trafficPageViewBean.getIsNew();
            }
        });
    }

    private SingleOutputStreamOperator<TrafficPageViewBean> withWaterMark(SingleOutputStreamOperator<TrafficPageViewBean> beanStream) {
        return beanStream
                .assignTimestampsAndWatermarks(WatermarkStrategy
                .<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(3L))
                .withTimestampAssigner(new SerializableTimestampAssigner<TrafficPageViewBean>() {
                    @Override
                    public long extractTimestamp(TrafficPageViewBean trafficPageViewBean, long l) {
                        return trafficPageViewBean.getTs();
                    }
                }));
    }

    private SingleOutputStreamOperator<TrafficPageViewBean> mapUvBean(SingleOutputStreamOperator<JSONObject> jsonObjStream) {
        return jsonObjStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("common").getString("mid");
            }
        }).process(new KeyedProcessFunction<String, JSONObject, TrafficPageViewBean>() {
            ValueState<String> lastLoginDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> lastLoginDtDes = new ValueStateDescriptor<String>("last_login_dt", String.class);
                // 设置状态的存活时间
                lastLoginDtDes.enableTimeToLive(StateTtlConfig
                        .newBuilder(org.apache.flink.api.common.time.Time.days(1L))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build());
                lastLoginDtState = getRuntimeContext().getState(lastLoginDtDes);
            }

            @Override
            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, TrafficPageViewBean>.Context context, Collector<TrafficPageViewBean> collector) throws Exception {
                // 判断独立访客
                // 当前数据的日期
                Long ts = jsonObject.getLong("ts");
                String curDt = DateFormatUtil.tsToDate(ts);
                JSONObject page = jsonObject.getJSONObject("page");
                JSONObject common = jsonObject.getJSONObject("common");
                String lastLoginDt = lastLoginDtState.value();
                Long uvCt = 0L;
                Long svCt = 0L;
                if (lastLoginDt == null || !lastLoginDt.equals(curDt)) {
                    // 状态没有存日期或者状态的日期和数据的日期不是同一天
                    // 当前是一条独立访客
                    uvCt = 1L;
                    lastLoginDtState.update(curDt);
                }

                // 判断会话数的方法
                // 判断last_page_id == null
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                if (lastPageId == null) {
                    // 新的会话
                    svCt = 1L;
                }
                collector.collect(TrafficPageViewBean.builder()
                        .vc(common.getString("vc"))
                        .ar(common.getString("ar"))
                        .ch(common.getString("ch"))
                        .isNew(common.getString("is_new"))
                        .uvCt(uvCt)
                        .svCt(svCt)
                        .pvCt(1L)
                        .durSum(page.getLong("during_time"))
                        .sid(common.getString("sid"))
                        .ts(ts)
                        .build());
            }
        });
    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    String mid = jsonObject.getJSONObject("common").getString("mid");
                    Long ts = jsonObject.getLong("ts");
                    if (mid != null && ts != null) {
                        collector.collect(jsonObject);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }
}
