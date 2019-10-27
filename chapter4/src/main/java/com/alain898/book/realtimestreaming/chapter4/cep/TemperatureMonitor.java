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

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by alain on 18/7/20.
 */
public class TemperatureMonitor {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<JSONObject> temperatureStream = env
                .addSource(new PeriodicSourceFunction())
                .assignTimestampsAndWatermarks(new EventTimestampPeriodicWatermarks())
                .setParallelism(1);

        Pattern<JSONObject, JSONObject> alarmPattern = Pattern.<JSONObject>begin("alarm")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        return value.getDouble("temperature") > 100.0d;
                    }
                })
                .times(2)
                .within(Time.seconds(15));

        DataStream<JSONObject> alarmStream = CEP.pattern(temperatureStream, alarmPattern)
                .select(new PatternSelectFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject select(Map<String, List<JSONObject>> pattern) throws Exception {
                        return pattern.get("alarm").stream()
                                .max(Comparator.comparingDouble(o -> o.getLongValue("temperature")))
                                .orElseThrow(() -> new IllegalStateException("should contains 2 events, but none"));
                    }
                }).setParallelism(1);

        Pattern<JSONObject, JSONObject> criticalPattern = Pattern.<JSONObject>begin("critical")
                .times(2)
                .within(Time.seconds(30));

        DataStream<JSONObject> criticalStream = CEP.pattern(alarmStream, criticalPattern)
                .flatSelect(new PatternFlatSelectFunction<JSONObject, JSONObject>() {
                    @Override
                    public void flatSelect(Map<String, List<JSONObject>> pattern,
                                           Collector<JSONObject> out) throws Exception {
                        List<JSONObject> critical = pattern.get("critical");
                        JSONObject first = critical.get(0);
                        JSONObject second = critical.get(1);
                        if (first.getLongValue("temperature") <
                                second.getLongValue("temperature")) {
                            JSONObject jsonObject = new JSONObject();
                            jsonObject.putAll(second);
                            out.collect(jsonObject);
                        }
                    }
                }).setParallelism(1);
//        temperatureStream.print().setParallelism(1);
//        alarmStream.print().setParallelism(1);
        criticalStream.print().setParallelism(1);
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
