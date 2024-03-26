package tech.dongfei.realtime.util;

import tech.dongfei.realtime.constant.Constant;

public class SQLUtil {
    public static String getKafkaSourceSQL(String topic, String groupId) {
        return "with(" +
                "  'connector' = 'kafka'," +
                "  'properties.group.id' = '" + groupId + "'," +
                "  'topic' = '" + topic + "'," +
                "  'properties.bootstrap.servers' = '" + Constant.KAFKA_BROKERS + "'," +
                "  'scan.startup.mode' = 'latest-offset'," +
                "  'json.ignore-parse-errors' = 'true'," +
                "  'format' = 'json' " +
                ")";
    }

    public static String getKafkaTopicDb(String groupId) {
        return "create table topic_db (" +
                "  `database` string, " +
                "  `table` string, " +
                "  `ts` bigint, " +
                "  `data` map<string, string>, " +
                "  `old` map<string, string>, " +
                "  `proc_time` as proctime(), " +
                ")" + getKafkaSourceSQL(Constant.TOPIC_DB, groupId);
    }
}
