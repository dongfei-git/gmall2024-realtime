package tech.dongfei.realtime.test;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static tech.dongfei.realtime.constant.Constant.*;


public class Test01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(MYSQL_HOST)
                .port(MYSQL_PORT)
                .username(MYSQL_USER_NAME)
                .password(MYSQL_PASSWORD)
                .databaseList("gmall_config")
                .tableList("gmall_config.table_process_dim")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();
        DataStreamSource<String> source = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source");
        source.print();
        env.execute();
    }
}
