package com.seedoilz.gmall.realtime.dim.app;

import com.seedoilz.gmall.realtime.common.base.BaseApp;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DimApp extends BaseApp {
    public static void main(String[] args) {
        new DimApp().start(10001,4, "dim_app", "topic_db");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        stream.print();
    }
}
