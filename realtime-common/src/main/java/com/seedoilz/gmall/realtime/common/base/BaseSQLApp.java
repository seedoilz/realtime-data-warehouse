package com.seedoilz.gmall.realtime.common.base;

import com.seedoilz.gmall.realtime.common.constant.Constant;
import com.seedoilz.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public abstract class BaseSQLApp {
    public abstract void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String groupId);

    public void start(int port, int parallelism, String ckAndGroupId) {
        System.setProperty("HADOOP_USER_NAME", "root");

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);


        // 设置并行度
        env.setParallelism(parallelism);

        // 设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        // 开启checkpoint
        env.enableCheckpointing(5000);

        // 设置checkpoint 模式 EXACTLY_ONCE
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // checkpoint存储
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall2023/stream/" + ckAndGroupId);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        env.getCheckpointConfig().setCheckpointTimeout(10000);

        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        handle(tableEnv, env, ckAndGroupId);
    }

    // 读取topic_db数据
    public void createTopicDb(String ckAndGroupId, StreamTableEnvironment tableEnv) {
        tableEnv.executeSql(SQLUtil.getKafkaTopicDb(ckAndGroupId));
    }

    // 读取base_dic字典表
    public void createBaseDic(StreamTableEnvironment tableEnv){
        tableEnv.executeSql(
                "create table base_dic (" +
                " rowkey string," +  // 如果字段是原子类型,则表示这个是 rowKey, 字段随意, 字段类型随意
                " info row<dic_name string>, " +  // 字段名和 hbase 中的列族名保持一致. 类型必须是 row. 嵌套进去的就是列
                " primary key (rowkey) not enforced " + // 只能用 rowKey 做主键
                ") WITH (" +
                " 'connector' = 'hbase-2.2'," +
                " 'table-name' = 'gmall:dim_base_dic'," +
                " 'zookeeper.quorum' = '" + Constant.HBASE_ZOOKEEPER_QUORUM + "' " +
//                " 'lookup.cache' = 'PARTIAL', " +
//                " 'lookup.async' = 'true', " +
//                " 'lookup.partial-cache.max-rows' = '20', " +
//                " 'lookup.partial-cache.expire-after-access' = '2 hour' " +
                ")"
        );
    }
}