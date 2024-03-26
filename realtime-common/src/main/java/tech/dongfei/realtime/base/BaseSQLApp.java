package tech.dongfei.realtime.base;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import tech.dongfei.realtime.constant.Constant;
import tech.dongfei.realtime.util.SQLUtil;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

public abstract class BaseSQLApp {
    public abstract void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String ckAndGroupId);

    public void start(int port, int parallelism, String ckAndGroupId) {
        // 1. 环境准备
        // 1.1 设置操作 Hadoop 的用户名为 Hadoop 超级用户 centos
        System.setProperty("HADOOP_USER_NAME", "centos");

        // 1.2 获取流处理环境，并指定本地测试时启动 WebUI 所绑定的端口
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // 1.3 设置并行度
        env.setParallelism(parallelism);

        // 1.4 状态后端及检查点相关配置
        // 1.4.1 设置状态后端
        env.setStateBackend(new HashMapStateBackend());

        // 1.4.2 开启 checkpoint
        env.enableCheckpointing(5000);
        // 1.4.3 设置 checkpoint 模式: 精准一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 1.4.4 checkpoint 存储
        env.getCheckpointConfig().setCheckpointStorage(Constant.HDFS_CHECKPOINT_STORAGE + ckAndGroupId);
        // 1.4.5 checkpoint 并发数
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 1.4.6 checkpoint 之间的最小间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        // 1.4.7 checkpoint  的超时时间
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        // 1.4.8 job 取消时 checkpoint 保留策略
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        handle(tableEnv, env, ckAndGroupId);

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 读取topic_db数据
    public void createTopicDb(String ckAndGroupId, StreamTableEnvironment tableEnv) {
        tableEnv.executeSql(SQLUtil.getKafkaTopicDb(ckAndGroupId));
    }

    public void createBaseDic(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql(
                "create table base_dic (" +
                        " rowkey string," +
                        " info row<dic_name string>, " +
                        " primary key (rowkey) not enforced " +
                        ") WITH (" +
                        " 'connector' = 'hbase-2.2'," +
                        " 'table-name' = 'gmall:dim_base_dic'," +
                        " 'zookeeper.quorum' = '" + Constant.HBASE_ZOOKEEPER_QUORUM + "', " +
                        " 'lookup.cache' = 'PARTIAL', " +
                        " 'lookup.async' = 'true', " +
                        " 'lookup.partial-cache.max-rows' = '20', " +
                        " 'lookup.partial-cache.expire-after-access' = '2 hour' " +
                        ")");
    }
}

