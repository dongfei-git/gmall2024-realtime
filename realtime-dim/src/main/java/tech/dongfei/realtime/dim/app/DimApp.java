package tech.dongfei.realtime.dim.app;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;
import tech.dongfei.realtime.base.BaseApp;
import tech.dongfei.realtime.bean.TableProcessDim;
import tech.dongfei.realtime.constant.Constant;
import tech.dongfei.realtime.dim.function.DimBroadcastFunction;
import tech.dongfei.realtime.dim.function.DimHBaseSinkFunction;
import tech.dongfei.realtime.util.FlinkSourceUtil;
import tech.dongfei.realtime.util.HBaseUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class DimApp extends BaseApp {
    public static void main(String[] args) {
        new DimApp().start(10001, 4, "dim_app", Constant.TOPIC_DB);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {

        //数据清洗
        SingleOutputStreamOperator<JSONObject> jsonObjStream = etl(stream);

        //使用flinkCDC读取监控配置表数据
        DataStreamSource<String> mysqlSource = env.fromSource(FlinkSourceUtil.getMysqlSource(Constant.PROCESS_DATABASE, Constant.PROCESS_TABLE),
                        WatermarkStrategy.noWatermarks(), "mysql_source")
                .setParallelism(1);

        //在HBASE中创建维度表
        SingleOutputStreamOperator<TableProcessDim> createTableStream = createHBaseTable(mysqlSource).setParallelism(1);

        //做成广播流，主流链接广播流
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimStream = connectionStream(createTableStream, jsonObjStream);

        //筛选出需要写出的字段
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> filterColumnStream = filterColumn(dimStream);

        //写出到HBASE
        filterColumnStream.print();
        filterColumnStream.addSink(new DimHBaseSinkFunction());
    }

    private static SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> filterColumn(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimStream) {
        return dimStream.map(new MapFunction<Tuple2<JSONObject, TableProcessDim>, Tuple2<JSONObject, TableProcessDim>>() {
            @Override
            public Tuple2<JSONObject, TableProcessDim> map(Tuple2<JSONObject, TableProcessDim> value) throws Exception {
                JSONObject jsonObject = value.f0;
                TableProcessDim dim = value.f1;
                String sinkColumns = dim.getSinkColumns();
                List<String> columns = Arrays.asList(sinkColumns.split(","));
                JSONObject data = jsonObject.getJSONObject("data");
                data.keySet().removeIf(key -> !columns.contains(key));
                return value;
            }
        });
    }

    private static SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> connectionStream(SingleOutputStreamOperator<TableProcessDim> createTableStream, SingleOutputStreamOperator<JSONObject> jsonObjStream) {
        MapStateDescriptor<String, TableProcessDim> broadcastState = new MapStateDescriptor<>("broadcast_state", String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastStateStream = createTableStream.broadcast(broadcastState);
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectStream = jsonObjStream.connect(broadcastStateStream);
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimStream = connectStream.process(new DimBroadcastFunction(broadcastState)).setParallelism(1);
        return dimStream;
    }

    private static SingleOutputStreamOperator<TableProcessDim> createHBaseTable(DataStreamSource<String> mysqlSource) {
        SingleOutputStreamOperator<TableProcessDim> createTableStream = mysqlSource.flatMap(new RichFlatMapFunction<String, TableProcessDim>() {
            public Connection connection;

            @Override
            public void open(Configuration parameters) throws Exception {
                connection = HBaseUtil.getConnection();
            }

            @Override
            public void flatMap(String value, Collector<TableProcessDim> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    String op = jsonObject.getString("op");
                    TableProcessDim tableProcessDim;
                    if ("d".equals(op)) {
                        tableProcessDim = jsonObject.getObject("before", TableProcessDim.class);
                        deleteTable(tableProcessDim);
                    } else if ("c".equals(op) || "r".equals(op)) {
                        tableProcessDim = jsonObject.getObject("after", TableProcessDim.class);
                        createTable(tableProcessDim);
                    } else {
                        tableProcessDim = jsonObject.getObject("after", TableProcessDim.class);
                        deleteTable(tableProcessDim);
                        createTable(tableProcessDim);
                    }
                    tableProcessDim.setOp(op);
                    collector.collect(tableProcessDim);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            private void createTable(TableProcessDim tableProcessDim) {
                try {
                    HBaseUtil.createTable(connection, Constant.HBASE_NAMESPACE, tableProcessDim.getSinkTable(), tableProcessDim.getSinkFamily().split(","));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            private void deleteTable(TableProcessDim tableProcessDim) {
                try {
                    HBaseUtil.dropTable(connection, Constant.HBASE_NAMESPACE, tableProcessDim.getSinkTable());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void close() throws Exception {
                HBaseUtil.closeConnection(connection);
            }
        });
        return createTableStream;
    }

    private static SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        SingleOutputStreamOperator<JSONObject> jsonObjStream = stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    String database = jsonObject.getString("database");
                    String type = jsonObject.getString("type");
                    JSONObject data = jsonObject.getJSONObject("data");
                    if ("gmall".equals(database) && !"bootstrap-start".equals(type)
                            && data != null && !data.isEmpty()) {
                        collector.collect(jsonObject);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        return jsonObjStream;
    }

}
