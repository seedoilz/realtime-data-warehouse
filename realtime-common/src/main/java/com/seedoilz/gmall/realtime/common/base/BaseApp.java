package com.seedoilz.gmall.realtime.common.base;

import com.seedoilz.gmall.realtime.common.util.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public abstract class BaseApp {
    public abstract void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream);

    public void start(int port, int parallelism, String ckAndGroupId, String topic){
        System.setProperty("HADOOP_USER_NAME", "root");

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);


        // 设置并行度
        env.setParallelism(parallelism);

//        // 设置状态后端
//        env.setStateBackend(new HashMapStateBackend());
//        // 开启checkpoint
//        env.enableCheckpointing(5000);
//
//        // 设置checkpoint 模式 EXACTLY_ONCE
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//
//        // checkpoint存储
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall2023/stream/" + ckAndGroupId);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
//        env.getCheckpointConfig().setCheckpointTimeout(10000);
//
//        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        KafkaSource<String> source = FlinkSourceUtil.getKafkaSource(ckAndGroupId, topic);

        DataStreamSource<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka_source");

        handle(env, stream);

        try{
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
