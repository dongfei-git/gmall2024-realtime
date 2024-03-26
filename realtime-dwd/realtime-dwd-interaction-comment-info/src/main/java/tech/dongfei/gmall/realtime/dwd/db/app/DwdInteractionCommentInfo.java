package tech.dongfei.gmall.realtime.dwd.db.app;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import tech.dongfei.realtime.base.BaseSQLApp;

public class DwdInteractionCommentInfo extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdInteractionCommentInfo().start(10012, 4, "dwd_interaction_comment_info");
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String ckAndGroupId) {
        createTopicDb(ckAndGroupId, tableEnv);
        createBaseDic(tableEnv);

        Table commentInfo = tableEnv.sqlQuery(
                "select " +
                        " `data`['id'] id, " +
                        " `data`['user_id'] user_id, " +
                        " `data`['sku_id'] sku_id, " +
                        " `data`['appraise'] appraise, " +
                        " `data`['comment_txt'] comment_txt, " +
                        " `data`['create_time'] comment_time," +
                        " ts, " +
                        " pt " +
                        " from topic_db " +
                        " where `database`='gmall' " +
                        " and `table`='comment_info' " +
                        " and `type`='insert' ");

        tableEnv.createTemporaryView("comment_info", commentInfo);

        Table result = tableEnv.sqlQuery("select " +
                "ci.id, " +
                "ci.user_id," +
                "ci.sku_id," +
                "ci.appraise," +
                "dic.info.dic_name appraise_name," +
                "ci.comment_txt," +
                "ci.ts " +
                "from comment_info ci " +
                "join base_dic for system_time as of ci.pt as dic " +
                "on ci.appraise=dic.dic_code");
    }
}
