package com.seedoilz.gmall.realtime.dwd.db.app;

import com.seedoilz.gmall.realtime.common.base.BaseSQLApp;
import com.seedoilz.gmall.realtime.common.constant.Constant;
import com.seedoilz.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class DwdTradeOrderDetail extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeOrderDetail().start(10014, 4, Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String groupId) {
        // 在flink设置TTL
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));
        // 核心逻辑
        // 1. 读取topic_db
        createTopicDb(groupId, tableEnv);

        // 2. 筛选订单详情表数据
        filterOdTable(tableEnv);

        // 3. 筛选订单信息表
        filterOiTable(tableEnv);

        // 4. 筛选订单详情活动关联表
        filterOdaTable(tableEnv);

        // 5. 筛选订单详情优惠券关联表
        filterOdcTable(tableEnv);

        // 6. 四张表Join
        Table joinTable = getJoinTable(tableEnv);

        // 7. Join的结果写出到Kafka中
        createUpsertKafkaSink(tableEnv);

        joinTable.insertInto(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL).execute();
    }

    private void createUpsertKafkaSink(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("create table " + Constant.TOPIC_DWD_TRADE_ORDER_DETAIL +" (\n" +
                "  id  STRING,\n" +
                "  order_id  STRING,\n" +
                "  sku_id  STRING,\n" +
                "  user_id  STRING,\n" +
                "  province_id  STRING,\n" +
                "  activity_id  STRING,\n" +
                "  activity_rule_id  STRING,\n" +
                "  coupon_id  STRING,\n" +
                "  sku_name  STRING,\n" +
                "  order_price  STRING,\n" +
                "  sku_num  STRING,\n" +
                "  create_time  STRING,\n" +
                "  split_total_amount  STRING,\n" +
                "  split_activity_amount  STRING,\n" +
                "  split_coupon_amount  STRING,\n" +
                "  ts bigint,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED \n" +
                ")" + SQLUtil.getUpsertKafkaSQL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL));
    }

    private Table getJoinTable(StreamTableEnvironment tableEnv) {
        return tableEnv.sqlQuery("select \n" +
                "  od.id,\n" +
                "  order_id,\n" +
                "  sku_id,\n" +
                "  user_id,\n" +
                "  province_id,\n" +
                "  activity_id,\n" +
                "  activity_rule_id,\n" +
                "  coupon_id,\n" +
                "  sku_name,\n" +
                "  order_price,\n" +
                "  sku_num,\n" +
                "  create_time,\n" +
                "  split_total_amount,\n" +
                "  split_activity_amount,\n" +
                "  split_coupon_amount,\n" +
                "  ts \n" +
                "from order_detail od\n" +
                "join order_info oi\n" +
                "on od.order_id = oi.id\n" +
                "left join order_detail_activity oda\n" +
                "on oda.id = od.id\n" +
                "left join order_detail_coupon odc\n" +
                "on odc.id = od.id ");
    }

    private void filterOdcTable(StreamTableEnvironment tableEnv) {
        Table odcTable = tableEnv.sqlQuery("select\n" +
                " `data`['order_detail_id'] id,\n" +
                " `data`['coupon_id'] coupon_id\n" +
                " from topic_db\n" +
                " where `database` = 'gmall'\n" +
                " and `table` = 'order_detail_coupon'\n" +
                " and `type` = 'insert'");

        tableEnv.createTemporaryView("order_detail_coupon", odcTable);
    }

    private void filterOdaTable(StreamTableEnvironment tableEnv) {
        Table odaTable = tableEnv.sqlQuery("select\n" +
                " `data`['order_detail_id'] id,\n" +
                " `data`['activity_id'] activity_id,\n" +
                " `data`['activity_rule_id'] activity_rule_id\n" +
                " from topic_db\n" +
                " where `database` = 'gmall'\n" +
                " and `table` = 'order_detail_activity'\n" +
                " and `type` = 'insert'");

        tableEnv.createTemporaryView("order_detail_activity", odaTable);
    }

    private void filterOiTable(StreamTableEnvironment tableEnv) {
        Table oiTable = tableEnv.sqlQuery("select\n" +
                " `data`['id'] id,\n" +
                " `data`['user_id'] user_id,\n" +
                " `data`['province_id'] province_id\n" +
                " from topic_db\n" +
                " where `database` = 'gmall'\n" +
                " and `table` = 'order_info'\n" +
                " and `type` = 'insert'");

        tableEnv.createTemporaryView("order_info", oiTable);
    }

    private void filterOdTable(StreamTableEnvironment tableEnv) {
        Table odTable = tableEnv.sqlQuery("select\n" +
                " `data`['id'] id,\n" +
                " `data`['order_id'] order_id,\n" +
                " `data`['sku_id'] sku_id,\n" +
                " `data`['sku_name'] sku_name,\n" +
                " `data`['order_price'] order_price,\n" +
                " `data`['sku_num'] sku_num,\n" +
                " `data`['create_time'] create_time,\n" +
                " `data`['split_total_amount'] split_total_amount,\n" +
                " `data`['split_activity_amount'] split_activity_amount,\n" +
                " `data`['split_coupon_amount'] split_coupon_amount,\n" +
                " ts\n" +
                " from topic_db\n" +
                " where `database` = 'gmall'\n" +
                " and `table` = 'order_detail'\n" +
                " and `type` = 'insert'");
        tableEnv.createTemporaryView("order_detail", odTable);
    }
}
