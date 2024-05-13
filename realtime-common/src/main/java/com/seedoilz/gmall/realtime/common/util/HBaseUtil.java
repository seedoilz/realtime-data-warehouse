package com.seedoilz.gmall.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseUtil {

    /**
     * 获取HBase链接
     * @return null
     */
    public static Connection getHBaseConnection() {
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return connection;
    }

    /**
     * 关闭连接
     * @param connection
     */
    public static void closeConnection(Connection connection) {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 创建HBase表格
     * @param connection
     * @param namespace
     * @param tableName
     * @param columnFamilyNames
     * @throws IOException
     */
    public static void createTable(Connection connection, String namespace, String tableName, String... columnFamilyNames) throws IOException {

        if (columnFamilyNames == null || columnFamilyNames.length == 0) {
            System.out.println("创建HBase至少得有一个列族");
        }

        // 1. 获取admin
        Admin admin = connection.getAdmin();
        // 2. 创建表格描述符
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(namespace, tableName));
        for (String columnFamilyName : columnFamilyNames) {
            ColumnFamilyDescriptor familyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamilyName)).build();
            tableDescriptorBuilder.setColumnFamily(familyDescriptor);
        }

        // 3. 使用admin调用方法创建表格
        try {
            admin.createTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
            System.out.println("当前表格已经存在  不需要重复创建" + namespace + ":" + tableName);
        }
        // 4. 关闭admin
        admin.close();
    }

    /**
     * 删除表格
     * @param connection
     * @param namespace
     * @param tableName
     * @throws IOException
     */
    public static void dropTable(Connection connection, String namespace, String tableName) throws IOException {
        // 1. 获取admin
        Admin admin = connection.getAdmin();
        try {
            admin.disableTable(TableName.valueOf(namespace, tableName));
            admin.deleteTable(TableName.valueOf(namespace, tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        admin.close();
    }


    /**
     * 写数据到HBase
     * @param connection
     * @param namespace
     * @param tableName
     * @param rowKey
     * @param familyName
     * @param jsonObject
     * @throws IOException
     */
    public static void putCells(Connection connection, String namespace, String tableName, String rowKey, String familyName, JSONObject jsonObject) throws IOException {
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        for (String column : jsonObject.keySet()) {
            String columnValue = jsonObject.getString(column);
            if (columnValue != null) {
                put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(column), Bytes.toBytes(columnValue));
            }
        }
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
        table.close();
    }

    /**
     * 删除一整行数据
     * @param connection
     * @param namespace
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    public static void deleteCells(Connection connection, String namespace, String tableName, String rowKey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        try {
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
