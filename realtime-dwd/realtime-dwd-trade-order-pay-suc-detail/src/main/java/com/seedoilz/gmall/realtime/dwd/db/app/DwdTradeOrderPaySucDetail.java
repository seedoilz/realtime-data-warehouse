package com.seedoilz.gmall.realtime.dwd.db.app;

import com.seedoilz.gmall.realtime.common.base.BaseSQLApp;
import com.seedoilz.gmall.realtime.common.constant.Constant;
import com.seedoilz.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class DwdTradeOrderPaySucDetail extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeOrderPaySucDetail().start(10016, 4, Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS);
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String groupId) {
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(15L));
        // 核心逻辑
        // 1. 创建topic_db
        createTopicDb(groupId, tableEnv);


        // 2. 筛选支付成功数据
        filterPaymentTable(tableEnv);


        // 3. 创建下单详情表数据
        createOrderDetailTable(tableEnv, groupId);


        // 4. 创建base_dic字典表
        createBaseDic(tableEnv);


        // 5. 使用interval join完成支付成功流和订完完成流的join
        intervalJoin(tableEnv);


        // 6. 使用lookup join完成维度退化
        tableEnv.executeSql("SELECT \n" +
                "  id,\n" +
                "  order_id,\n" +
                "  user_id,\n" +
                "  payment_type payment_type_code,\n" +
                "  info.dic_name payment_type_name,\n" +
                "  payment_time,\n" +
                "  sku_id,\n" +
                "  province_id,\n" +
                "  activity_id,\n" +
                "  activity_rule_id,\n" +
                "  coupon_id,\n" +
                "  sku_name,\n" +
                "  order_price,\n" +
                "  sku_num,\n" +
                "  split_total_amount,\n" +
                "  split_activity_amount,\n" +
                "  split_coupon_amount,\n" +
                "  ts\n" +
                "FROM pay_order p\n" +
                "left join base_dic  FOR SYSTEM_TIME AS OF p.proc_time  as b\n" +
                "on p.payment_type = b.rowkey").print();
        Table resultTable = lookUpJoin(tableEnv);


        // 7. 创建upsert kafka写出
        createUpsertKafkaSink(tableEnv);


        resultTable.insertInto(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS).execute();
    }

    private void createUpsertKafkaSink(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("create table " + Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS + "(\n" +
                "  id STRING,\n" +
                "  order_id STRING,\n" +
                "  user_id STRING,\n" +
                "  payment_type_code STRING,\n" +
                "  payment_type_name STRING,\n" +
                "  payment_time STRING,\n" +
                "  sku_id STRING,\n" +
                "  province_id STRING,\n" +
                "  activity_id STRING,\n" +
                "  activity_rule_id STRING,\n" +
                "  coupon_id STRING,\n" +
                "  sku_name STRING,\n" +
                "  order_price STRING,\n" +
                "  sku_num STRING,\n" +
                "  split_total_amount STRING,\n" +
                "  split_activity_amount STRING,\n" +
                "  split_coupon_amount STRING,\n" +
                "  ts bigint ,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED \n" +
                ")" + SQLUtil.getUpsertKafkaSQL(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS));
    }

    private Table lookUpJoin(StreamTableEnvironment tableEnv) {
        return tableEnv.sqlQuery("SELECT \n" +
                "  id,\n" +
                "  order_id,\n" +
                "  user_id,\n" +
                "  payment_type payment_type_code,\n" +
                "  info.dic_name payment_type_name,\n" +
                "  payment_time,\n" +
                "  sku_id,\n" +
                "  province_id,\n" +
                "  activity_id,\n" +
                "  activity_rule_id,\n" +
                "  coupon_id,\n" +
                "  sku_name,\n" +
                "  order_price,\n" +
                "  sku_num,\n" +
                "  split_total_amount,\n" +
                "  split_activity_amount,\n" +
                "  split_coupon_amount,\n" +
                "  ts\n" +
                "FROM pay_order p\n" +
                "left join base_dic  FOR SYSTEM_TIME AS OF p.proc_time  as b\n" +
                "on p.payment_type = b.rowkey");
    }

    private void intervalJoin(StreamTableEnvironment tableEnv) {
        Table payOrderTable = tableEnv.sqlQuery("SELECT \n" +
                "  od.id,\n" +
                "  p.order_id,\n" +
                "  p.user_id,\n" +
                "  payment_type,\n" +
                "  callback_time payment_time,\n" +
                "  sku_id,\n" +
                "  province_id,\n" +
                "  activity_id,\n" +
                "  activity_rule_id,\n" +
                "  coupon_id,\n" +
                "  sku_name,\n" +
                "  order_price,\n" +
                "  sku_num,\n" +
                "  split_total_amount,\n" +
                "  split_activity_amount,\n" +
                "  split_coupon_amount,\n" +
                "  p.ts,\n" +
                "  p.proc_time\n" +
                "  FROM payment p, order_detail od\n" +
                "  WHERE p.order_id = od.order_id\n" +
                "  AND p.row_time BETWEEN od.row_time - INTERVAL '15' MINUTE AND od.row_time + INTERVAL '5' SECOND");

        tableEnv.createTemporaryView("pay_order", payOrderTable);
    }

    private void createOrderDetailTable(StreamTableEnvironment tableEnv, String groupId) {
        tableEnv.executeSql("create table order_detail (\n" +
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
                "  row_time as TO_TIMESTAMP_LTZ(ts * 1000,3), \n" +
                "  WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND \n" +
                "  )" + SQLUtil.getKafkaSourceSQL(Constant.TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS, groupId));
    }

    private void filterPaymentTable(StreamTableEnvironment tableEnv) {
        Table paymentTable = tableEnv.sqlQuery("select " +
                "  `data`['id'] id,\n" +
                "  `data`['order_id'] order_id,\n" +
                "  `data`['user_id'] user_id,\n" +
                "  `data`['payment_type'] payment_type,\n" +
                "  `data`['total_amount'] total_amount,\n" +
                "  `data`['callback_time'] callback_time,\n" +
                "   ts,\n" +
                "   row_time,\n" +
                "   proc_time\n" +
                "  from topic_db " +
                "  where `database`='gmall' " +
                "  and `table`='payment_info' " +
                "  and `type`='update' " +
                "  and `old`['payment_status'] is not null " +
                "  and `data`['payment_status']='1602' ");

        tableEnv.createTemporaryView("payment", paymentTable);
    }
}
