import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test02 {
    public static void main(String[] args) throws Exception {
        // TODO 1. 初始化流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2. 检查点
        // 2.1 启用检查点
        env.enableCheckpointing(10 * 1000L);
        // 2.2 设置相邻两次检查点最小间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30 * 1000L);
        // 2.3 设置取消 Job 时检查点的清理模式
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        // 2.4 设置状态后端类型
        env.setStateBackend(new HashMapStateBackend());
        // 2.5 设置检查点存储路径
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/flinkCDC");
        // 2.6 设置 HDFS 用户名
        System.setProperty("HADOOP_USER_NAME", "root");

        // TODO 3. 创建 Flink-MySQL-CDC 的 Source
        // initial:Performs an initial snapshot on the monitored database tables upon first startup, and continue to read the latest binlog.
        // earliest:Never to perform snapshot on the monitored database tables upon first startup, just read from the beginning of the binlog. This should be used with care, as it is only valid when the binlog is guaranteed to contain the entire history of the database.
        // latest:Never to perform snapshot on the monitored database tables upon first startup, just read from the end of the binlog which means only have the changes since the connector was started.
        // specificOffset:Never to perform snapshot on the monitored database tables upon first startup, and directly read binlog from the specified offset.
        // timestamp:Never to perform snapshot on the monitored database tables upon first startup, and directly read binlog from the specified timestamp.The consumer will traverse the binlog from the beginning and ignore change events whose timestamp is smaller than the specified timestamp.
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("gmall2023_config") // set captured database
                .tableList("gmall2023_config.table_process_dim") // set captured table
                .username("root")
                .password("Czy026110")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .startupOptions(StartupOptions.initial())
                .build();

        // TODO 4.使用CDC Source从MySQL读取数据
        DataStreamSource<String> mysqlDS =
                env.fromSource(
                        mySqlSource,
                        WatermarkStrategy.noWatermarks(),
                        "mysql_source").setParallelism(1);

        // TODO 5.打印输出
        mysqlDS.print();

        // TODO 6.执行任务
        env.execute();
    }
}