package tech.dongfei.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;
import tech.dongfei.realtime.bean.TableProcessDim;
import tech.dongfei.realtime.constant.Constant;
import tech.dongfei.realtime.util.HBaseUtil;

import java.io.IOException;

public class DimHBaseSinkFunction extends RichSinkFunction<Tuple2<JSONObject, TableProcessDim>> {

    Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = HBaseUtil.getConnection();
    }

    @Override
    public void close() throws Exception {
        HBaseUtil.closeConnection(connection);
    }

    @Override
    public void invoke(Tuple2<JSONObject, TableProcessDim> value, Context context) throws Exception {
        JSONObject jsonObject = value.f0;
        TableProcessDim dim = value.f1;
        JSONObject data = jsonObject.getJSONObject("data");
        String type = jsonObject.getString("type");  //insert update delete bootstrap-insert
        if ("delete".equals(type)) {
            delete(data, dim);
        } else {
            put(data, dim);
        }
    }

    private void put(JSONObject data, TableProcessDim dim) {
        String sinkTable = dim.getSinkTable();
        String sinkRowKeyName = dim.getSinkRowKey();
        String sinkRowKeyValue = data.getString(sinkRowKeyName);
        String sinkFamily = dim.getSinkFamily();
        try {
            HBaseUtil.putCells(connection, Constant.HBASE_NAMESPACE, sinkTable, sinkRowKeyValue, sinkFamily, data);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void delete(JSONObject data, TableProcessDim dim) {
        String sinkTable = dim.getSinkTable();
        String sinkRowKeyName = dim.getSinkRowKey();
        String sinkRowKeyValue = data.getString(sinkRowKeyName);
        try {
            HBaseUtil.deleteCells(connection, Constant.HBASE_NAMESPACE, sinkTable, sinkRowKeyValue);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
