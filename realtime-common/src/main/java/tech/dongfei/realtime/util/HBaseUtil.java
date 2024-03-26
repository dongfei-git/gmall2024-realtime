package tech.dongfei.realtime.util;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import tech.dongfei.realtime.constant.Constant;

import java.io.IOException;

public class HBaseUtil {

    /**
     * 获取HBASE链接
     * @return
     */
    public static Connection getConnection() {
        Configuration configuration = new Configuration();
        configuration.set("hbase.zookeeper.quorum", Constant.HBASE_ZOOKEEPER_QUORUM);
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return connection;
    }

    /**
     * 关闭连接
     * @param connection
     */
    public static void closeConnection(Connection connection) {
        if (connection != null && !connection.isClosed()) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 创建表格
     * @param connection  连接
     * @param namespace  命名空间
     * @param table  表名
     * @param familys  列族
     * @throws IOException  异常
     */
    public static void createTable(Connection connection, String namespace, String table, String... familys) throws IOException {
        if (familys.length == 0) {
            System.out.println("创建HBASE表格至少有一个列族");
            return;
        }

        Admin admin = connection.getAdmin();
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(namespace, table));
        for (String family : familys) {
            ColumnFamilyDescriptor familyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family)).build();
            tableDescriptorBuilder.setColumnFamily(familyDescriptor);
        }
        try {
            admin.createTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
            System.out.println("当前表格已经存在，不需要重复创建" + namespace + ":" + table);
        }
        admin.close();
    }

    /**
     * 删除表格
     * @param connection
     * @param namespace
     * @param table
     * @throws IOException
     */
    public static void dropTable(Connection connection,String namespace, String table) throws IOException {
        Admin admin = connection.getAdmin();
        try {
            admin.disableTable(TableName.valueOf(namespace,table));
            admin.deleteTable(TableName.valueOf(namespace,table));
        } catch (IOException e) {
            e.printStackTrace();
        }
        admin.close();
    }

    /**
     * 写入数据到HBASE
     * @param connection
     * @param namespace
     * @param tableName
     * @param rowKey
     * @param family
     * @param data
     * @throws IOException
     */
    public static void putCells(Connection connection, String namespace, String tableName,
            String rowKey, String family, JSONObject data) throws IOException {
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        for (String column : data.keySet()) {
            String columnValue = data.getString(column);
            if (columnValue != null) {
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column),Bytes.toBytes(columnValue));
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
     * 删除数据
     * @param connection
     * @param namespace
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    public static void deleteCells(Connection connection, String namespace, String tableName,
                                   String rowKey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        try {
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        }
        table.close();
    }
}
