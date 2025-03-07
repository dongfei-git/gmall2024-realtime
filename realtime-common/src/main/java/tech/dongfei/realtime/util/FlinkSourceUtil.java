package tech.dongfei.realtime.util;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import tech.dongfei.realtime.constant.Constant;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static tech.dongfei.realtime.constant.Constant.*;
import static tech.dongfei.realtime.constant.Constant.MYSQL_PASSWORD;

public class FlinkSourceUtil {
    public static KafkaSource<String> getKafkaSource(String groupId,
                                                     String topic) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setGroupId(groupId)
                .setTopics(topic)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    @Override
                    public String deserialize(byte[] message) throws IOException {
                        if (message != null) {
                            return new String(message, StandardCharsets.UTF_8);
                        }
                        return null;
                    }

                    @Override
                    public boolean isEndOfStream(String nextElement) {
                        return false;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return Types.STRING;
                    }
                })
                .build();
    }

    public static MySqlSource<String> getMysqlSource(String databaseName, String tableName) {
        Properties properties = new Properties();
        properties.setProperty("useSSL", "false");
        properties.setProperty("allowPublicKeyRetrieval", "true");

        return MySqlSource.<String>builder()
                .hostname(MYSQL_HOST)
                .port(MYSQL_PORT)
                .username(MYSQL_USER_NAME)
                .password(MYSQL_PASSWORD)
                .jdbcProperties(properties)
                .databaseList(databaseName)
                .tableList(databaseName+"."+tableName)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();
    }
}

