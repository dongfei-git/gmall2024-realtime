package tech.dongfei.realtime.dwd.db.split.app;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import tech.dongfei.realtime.base.BaseApp;
import tech.dongfei.realtime.constant.Constant;
import tech.dongfei.realtime.util.DateFormatUtil;
import tech.dongfei.realtime.util.FlinkSinkUtil;

import java.time.Duration;

public class DwdBaseLog extends BaseApp {

    public static void main(String[] args) {
        new DwdBaseLog().start(10011, 4, "dwd_base_log", Constant.TOPIC_LOG);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        //ETL过滤不完整的数据
        SingleOutputStreamOperator<JSONObject> jsonObjStream = etl(stream);

        //新旧访客修复
        KeyedStream<JSONObject, String> keyedStream = keyByWithWaterMark(jsonObjStream);
        SingleOutputStreamOperator<JSONObject> isNewFixStream = isNewFix(keyedStream);

        //拆分不同类型的用户行为日志
        //启动日志：启动信息、报错信息
        //页面日志：页面信息(主流)、曝光信息、动作信息、报错信息
//        OutputTag<String> startTag = new OutputTag<String>("start"){};
        OutputTag<String> startTag = new OutputTag<String>("start", TypeInformation.of(String.class));
        OutputTag<String> errorTag = new OutputTag<String>("error", TypeInformation.of(String.class));
        OutputTag<String> displayTag = new OutputTag<String>("display", TypeInformation.of(String.class));
        OutputTag<String> actionTag = new OutputTag<String>("action", TypeInformation.of(String.class));
        SingleOutputStreamOperator<String> pageStream = splitLog(isNewFixStream, errorTag, startTag, displayTag, actionTag);


        SideOutputDataStream<String> startStream = pageStream.getSideOutput(startTag);
        SideOutputDataStream<String> errorStream = pageStream.getSideOutput(errorTag);
        SideOutputDataStream<String> displayStream = pageStream.getSideOutput(displayTag);
        SideOutputDataStream<String> actionStream = pageStream.getSideOutput(actionTag);

        pageStream.print("pageStream");
        startStream.print("startStream");
        errorStream.print("errorStream");
        displayStream.print("displayStream");
        actionStream.print("actionStream");

        //写出到kafka
        pageStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
        startStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
        errorStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
        displayStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
        actionStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));

    }

    private static SingleOutputStreamOperator<String> splitLog(SingleOutputStreamOperator<JSONObject> isNewFixStream, OutputTag<String> errorTag, OutputTag<String> startTag, OutputTag<String> displayTag, OutputTag<String> actionTag) {
        return isNewFixStream.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) throws Exception {
                JSONObject err = jsonObject.getJSONObject("err");
                if (err != null) {
                    context.output(errorTag, err.toJSONString());
                    jsonObject.remove("err");
                }
                JSONObject page = jsonObject.getJSONObject("page");
                JSONObject start = jsonObject.getJSONObject("start");
                JSONObject common = jsonObject.getJSONObject("common");
                Long ts = jsonObject.getLong("ts");
                if (start != null) {
                    context.output(startTag, jsonObject.toJSONString());
                } else if (page != null) {
                    JSONArray displays = jsonObject.getJSONArray("displays");
                    if (displays != null) {
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            display.put("common", common);
                            display.put("ts", ts);
                            display.put("page", page);
                            context.output(displayTag, display.toJSONString());
                        }
                        jsonObject.remove("displays");
                    }

                    JSONArray actions = jsonObject.getJSONArray("actions");
                    if (actions != null) {
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject action = actions.getJSONObject(i);
                            action.put("common", common);
                            action.put("ts", ts);
                            action.put("page", page);
                            context.output(actionTag, action.toJSONString());
                        }
                        jsonObject.remove("actions");
                    }

                    collector.collect(jsonObject.toJSONString());
                } else {

                }

            }
        });
    }

    private static SingleOutputStreamOperator<JSONObject> isNewFix(KeyedStream<JSONObject, String> keyedStream) {
        return keyedStream.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
            ValueState<String> firstLoginDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                firstLoginDtState = getRuntimeContext().getState(new ValueStateDescriptor<String>("first_login_dt", String.class));
            }

            @Override
            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                String is_new = jsonObject.getJSONObject("common").getString("is_new");
                Long ts = jsonObject.getLong("ts");
                String curDt = DateFormatUtil.tsToDate(ts);
                String firstLoginDt = firstLoginDtState.value();
                if ("1".equals(is_new)) {
                    if (firstLoginDt != null && !firstLoginDt.equals(curDt)) {
                        //伪装新访客处理
                        jsonObject.getJSONObject("common").put("is_new", "0");
                    } else if (firstLoginDt == null) {
                        //新访客处理
                        firstLoginDtState.update(curDt);
                    } else {
                        //新访客重复登录
                    }
                } else if ("0".equals(is_new)) {
                    //旧访客登录
                    if (firstLoginDt == null) {
                        //flink没有记录这个访客, 更新为昨天的日期
                        firstLoginDtState.update(DateFormatUtil.tsToDate(ts - 24 * 60 * 60 * 1000L));
                    } else {
                        //正常情况
                    }
                } else {
                    //当前数据错误
                }
                collector.collect(jsonObject);
            }
        });
    }

    private static KeyedStream<JSONObject, String> keyByWithWaterMark(SingleOutputStreamOperator<JSONObject> jsonObjStream) {
        return jsonObjStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3L))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject jsonObject, long l) {
                                return jsonObject.getLong("ts");
                            }
                        }))
                .keyBy(new KeySelector<JSONObject, String>() {
                    @Override
                    public String getKey(JSONObject jsonObject) throws Exception {
                        return jsonObject.getJSONObject("common").getString("mid");
                    }
                });
    }

    private static SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    JSONObject page = jsonObject.getJSONObject("page");
                    JSONObject start = jsonObject.getJSONObject("start");
                    JSONObject common = jsonObject.getJSONObject("common");
                    Long ts = jsonObject.getLong("ts");
                    if (page != null || start != null) {
                        if (common != null && common.getString("mid") != null && ts != null) {
                            collector.collect(jsonObject);
                        }
                    }
                } catch (Exception e) {
                    System.out.println("过滤掉的脏数据: " + value);
                }
            }
        });
    }
}
