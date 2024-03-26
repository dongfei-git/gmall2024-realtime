package tech.dongfei.realtime.constant;

public class Constant {
    public static final String KAFKA_BROKERS = "realtime-1:9092,realtime-2:9092,realtime-3:9092";
    public static final String HDFS_CHECKPOINT_STORAGE = "hdfs://realtime-1:8020/gmall2024/stream/";
    public static final String TOPIC_DB = "topic_db";
    public static final String TOPIC_LOG = "topic_log";
    public static final String MYSQL_HOST = "realtime-mysql";
    public static final int MYSQL_PORT = 3306;
    public static final String MYSQL_USER_NAME = "root";
    public static final String MYSQL_PASSWORD = "Dongfei.Tech@123";
    public static final String PROCESS_DATABASE = "gmall_config";
    public static final String PROCESS_TABLE = "table_process_dim";
    public static final String HBASE_ZOOKEEPER_QUORUM = "realtime-1,realtime-2,realtime-3";
    public static final String HBASE_NAMESPACE = "gmall";
    public static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    public static final String MYSQL_URL = "jdbc:mysql://realtime-mysql:3306?useSSL=false";
    public static final String TOPIC_DWD_TRAFFIC_START = "dwd_traffic_start";
    public static final String TOPIC_DWD_TRAFFIC_ERR = "dwd_traffic_err";
    public static final String TOPIC_DWD_TRAFFIC_PAGE = "dwd_traffic_page";
    public static final String TOPIC_DWD_TRAFFIC_ACTION = "dwd_traffic_action";
    public static final String TOPIC_DWD_TRAFFIC_DISPLAY = "dwd_traffic_display";
    public static final String TOPIC_DWD_INTERACTION_COMMENT_INFO = "dwd_interaction_comment_info";
    public static final String TOPIC_DWD_TRADE_CART_ADD = "dwd_trade_cart_add";
    public static final String TOPIC_DWD_TRADE_ORDER_DETAIL = "dwd_trade_order_detail";
    public static final String TOPIC_DWD_TRADE_ORDER_CANCEL = "dwd_trade_order_cancel";
    public static final String TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS = "dwd_trade_order_payment_success";
    public static final String TOPIC_DWD_TRADE_ORDER_REFUND = "dwd_trade_order_refund";
    public static final String TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS = "dwd_trade_refund_payment_success";
    public static final String TOPIC_DWD_USER_REGISTER = "dwd_user_register";

}

