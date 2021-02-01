package com.alain898.book.realtimestreaming.chapter4.cep;

import com.alain898.book.realtimestreaming.common.TimeTools;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class Demo {
    private static final Logger logger = LoggerFactory.getLogger(TimeTools.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<JSONObject> temperatureStream = env
                .addSource(new PeriodicSourceFunction())
                .assignTimestampsAndWatermarks(new EventTimestampPeriodicWatermarks())
                .setParallelism(1);

        Pattern<JSONObject, JSONObject> startPattern = Pattern.<JSONObject>begin("start")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        return value.getDouble("temperature") < 100.0d;
                    }
                })
                .times(3);
//                .within(Time.seconds(15));

//        Pattern<JSONObject, JSONObject> untilPattern = startPattern
//                .timesOrMore(2)
//                .until(new SimpleCondition<JSONObject>() {
//                    @Override
//                    public boolean filter(JSONObject value) throws Exception {
//                        return value.getDouble("temperature") >= 100.0d;
//                    }
//                });
//                .within(Time.seconds(15));

//        Pattern<JSONObject, JSONObject> nextPattern = startPattern.next("next")
//                .where(new SimpleCondition<JSONObject>() {
//                    @Override
//                    public boolean filter(JSONObject value) throws Exception {
//                        return value.getDouble("temperature") > 100.0d;
//                    }
//                })
//                .times(3);
//                .within(Time.seconds(15));
////
//        Pattern<JSONObject, JSONObject> followedByPattern = startPattern.followedBy("followedBy")
//                .where(new SimpleCondition<JSONObject>() {
//                    @Override
//                    public boolean filter(JSONObject value) throws Exception {
//                        return value.getDouble("temperature") > 100.0d;
//                    }
//                })
//                .times(3);
//                .within(Time.seconds(15));

//        Pattern<JSONObject, JSONObject> notNextPattern = startPattern.notNext("notNext")
//                .where(new SimpleCondition<JSONObject>() {
//                    @Override
//                    public boolean filter(JSONObject value) throws Exception {
//                        return value.getDouble("temperature") > 100.0d;
//                    }
//                });
//                .times(3);
//                .within(Time.seconds(15));

        Pattern<JSONObject, JSONObject> notFollowedByPattern = startPattern.notFollowedBy("notFollowedBy")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        return value.getDouble("temperature") > 100.0d;
                    }
                });
//                .times(3);
//                .within(Time.seconds(15));

        Pattern<JSONObject, JSONObject> followedByPattern2 = notFollowedByPattern.followedBy("followedBy")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        return value.getDouble("temperature") > 100.0d;
                    }
                })
                .times(1);

        DataStream<JSONObject> alarmStream = CEP.pattern(temperatureStream, followedByPattern2)
                .select(new PatternSelectFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject select(Map<String, List<JSONObject>> pattern) throws Exception {
//                        logger.info(String.format("start size[%d], next size[%d]",
//                                pattern.get("start").size(), pattern.get("next").size()));
//                        return pattern.get("alarm").stream()
                        JSONObject result = new JSONObject();
                        result.put("start", pattern.get("start"));
                        result.put("next", pattern.get("next"));
                        result.put("followedBy", pattern.get("followedBy"));
                        result.put("notNext", pattern.get("notNext"));
                        result.put("notFollowedBy", pattern.get("notFollowedBy"));
                        return result;
                    }
                }).setParallelism(1);
        alarmStream.print().setParallelism(1);
        env.execute();
    }

    private static class PeriodicSourceFunction extends RichSourceFunction<JSONObject> {

        private final AtomicLong id = new AtomicLong();
        private volatile boolean running = true;

        PeriodicSourceFunction() {
        }

        public void run(SourceContext<JSONObject> ctx) throws Exception {
            while (running) {
                TimeTools.sleepSec(1);
                ctx.collect(buildEvent());
            }
        }

        private JSONObject buildEvent() {
            JSONObject ret = new JSONObject();
            double temperature = RandomUtils.nextDouble(0, 200);
            ret.put("id", id.getAndIncrement());
            ret.put("timestamp", System.currentTimeMillis());
            ret.put("temperature", temperature);
            return ret;
        }

        public void cancel() {
            running = false;
        }
    }

    private static class EventTimestampPeriodicWatermarks implements AssignerWithPeriodicWatermarks<JSONObject> {
        private long currentMaxTimestamp;

        @Override
        public long extractTimestamp(JSONObject element, long previousElementTimestamp) {
            Long timestamp = element.getLong("timestamp");
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            return timestamp;
        }

        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxTimestamp);
        }
    }

}
