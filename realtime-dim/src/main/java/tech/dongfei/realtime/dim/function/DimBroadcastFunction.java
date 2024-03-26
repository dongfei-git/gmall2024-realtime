package tech.dongfei.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import tech.dongfei.realtime.bean.TableProcessDim;
import tech.dongfei.realtime.util.JdbcUtil;

import java.util.HashMap;
import java.util.List;

public class DimBroadcastFunction extends BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>> {
    public HashMap<String, TableProcessDim> hashMap = new HashMap<>();
    public MapStateDescriptor<String, TableProcessDim> broadcastState;

    public DimBroadcastFunction(MapStateDescriptor<String, TableProcessDim> broadcastState) {
        this.broadcastState = broadcastState;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //预加载初始的维度表信息
        java.sql.Connection mysqlConnection = JdbcUtil.getMysqlConnection();
        List<TableProcessDim> tableProcessDimList = JdbcUtil.queryList(mysqlConnection, "select * from gmall_config.table_process_dim", TableProcessDim.class, true);
        for (TableProcessDim tableProcessDim : tableProcessDimList) {
            tableProcessDim.setOp("r");
            hashMap.put(tableProcessDim.getSourceTable(), tableProcessDim);
        }
        JdbcUtil.closeConnection(mysqlConnection);
    }

    /**
     * 读取广播状态，将配置表信息作为一个维度表标记存放到广播状态
     * @param tableProcessDim
     * @param context
     * @param collector
     * @throws Exception
     */
    @Override
    public void processBroadcastElement(TableProcessDim tableProcessDim, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.Context context, Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {
        BroadcastState<String, TableProcessDim> tableProcessState = context.getBroadcastState(broadcastState);
        String op = tableProcessDim.getOp();
        if ("d".equals(op)) {
            tableProcessState.remove(tableProcessDim.getSourceTable());
            hashMap.remove(tableProcessDim.getSourceTable());
        } else {
            tableProcessState.put(tableProcessDim.getSourceTable(), tableProcessDim);
        }
    }

    /**
     * 处理主流数据
     * @param jsonObject
     * @param readOnlyContext
     * @param collector
     * @throws Exception
     */
    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<JSONObject, TableProcessDim>> collector) throws Exception {
        ReadOnlyBroadcastState<String, TableProcessDim> tableProcessState = readOnlyContext.getBroadcastState(broadcastState);
        String tableName = jsonObject.getString("table");
        TableProcessDim tableProcessDim = tableProcessState.get(tableName);
        if (tableProcessDim == null) {
            tableProcessDim = hashMap.get(tableName);
        }

        if (tableProcessDim != null) {
            collector.collect(Tuple2.of(jsonObject, tableProcessDim));
        }
    }
}
