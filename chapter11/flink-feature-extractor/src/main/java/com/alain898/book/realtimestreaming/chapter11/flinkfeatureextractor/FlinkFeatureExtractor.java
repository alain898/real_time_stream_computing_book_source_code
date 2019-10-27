package com.alain898.book.realtimestreaming.chapter11.flinkfeatureextractor;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class FlinkFeatureExtractor {

    private static final Set<String> keys = new HashSet<>(Lists.newArrayList(
            "amount#user_id.history", "device_id#user_id.history", "device_id.history"));

//    // maybe for dsl support
//    private static final Set<String> features = new HashSet<>(Lists.newArrayList(
//            "sum(amount#user.history,1d)",
//            "count_distinct(device#user.history,7d)",
//            "count(device.history,7d)"));

    private static final List<String[]> features2 = Arrays.asList(
            new String[]{"amount#user_id.history", "sum", "1d"},
            new String[]{"device_id#user_id.history", "count_distinct", "7d"},
            new String[]{"device_id.history", "count", "7d"}
    );

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(5000); // checkpoint机制会允许失败重试从而隐藏程序bug，在开发和调试阶段可以关闭

        FlinkKafkaConsumer010<String> myConsumer = createKafkaConsumer();
        DataStream<String> stream = env.addSource(myConsumer);

        DataStream counts = stream
                .map(new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String s) throws Exception {
                        if (StringUtils.isEmpty(s)) {
                            return new JSONObject();
                        }
                        return JSONObject.parseObject(s);
                    }
                })
                .flatMap(new FlatMapFunction<JSONObject, JSONObject>() {
                    @Override
                    public void flatMap(JSONObject value, Collector<JSONObject> out) throws Exception {
                        String eventId = UUID.randomUUID().toString();
                        long timestamp = value.getLongValue("timestamp");
                        JSONObject event = new JSONObject();
                        event.put("KEY_NAME", "event");
                        event.put("KEY_VALUE", eventId);
                        event.put("EVENT_ID", eventId);
                        event.putAll(value);
                        out.collect(event);
                        keys.forEach(key -> {
                            JSONObject json = new JSONObject();
                            json.put("timestamp", timestamp);
                            json.put("KEY_NAME", key);
                            json.put("KEY_VALUE", genKeyValue(value, key));
                            json.put("EVENT_ID", eventId);
                            genKeyFields(key).forEach(f -> json.put(f, value.get(f)));
                            out.collect(json);
                        });
                    }
                })
                .keyBy(new KeySelector<JSONObject, String>() {
                    @Override
                    public String getKey(JSONObject value) throws Exception {
                        return value.getString("KEY_VALUE");
                    }
                })
                .map(new KeyEnrichFunction())
                .map(new FeatureEnrichFunction())
                .keyBy(new KeySelector<JSONObject, String>() {

                    @Override
                    public String getKey(JSONObject value) throws Exception {
                        return value.getString("EVENT_ID");
                    }
                })
                .flatMap((new FeatureReduceFunction()));

        counts.print().setParallelism(1);
        env.execute("flink feature extractor");

    }


    public static class KeyEnrichFunction extends RichMapFunction<JSONObject, JSONObject> {

        private ValueState<Serializable> keyState;

        @Override
        public void open(Configuration config) {
            keyState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved keyState", Serializable.class));
        }

        private <T> T getState(Class<T> tClass) throws IOException {
            return tClass.cast(keyState.value());
        }

        private void setState(Serializable v) throws IOException {
            keyState.update(v);
        }

        @Override
        public JSONObject map(JSONObject event) throws Exception {
            String keyName = event.getString("KEY_NAME");

            if (keyName.equals("event")) {
                return event;
            }

            if (keyName.endsWith(".history")) {
                JSONArray history = getState(JSONArray.class);
                if (history == null) {
                    history = new JSONArray();
                }
                history.add(event);
                if (history.size() > 100) {
                    history.remove(0);
                }
                setState(history);
                JSONObject newEvent = new JSONObject();
                newEvent.putAll(event);
                newEvent.put("HISTORY", history);
                return newEvent;
            } else {
                throw new UnsupportedOperationException("unsupported key type");
            }
        }
    }

    public static class FeatureEnrichFunction extends RichMapFunction<JSONObject, JSONObject> {
        private static final List<String[]> features = FlinkFeatureExtractor.features2;

        @Override
        public JSONObject map(JSONObject value) throws Exception {
            String keyName = value.getString("KEY_NAME");
            if (keyName.equals("event")) {
                return value;
            }

            for (String[] feature : features) {
                String key = feature[0];
                if (!StringUtils.equals(key, keyName)) {
                    continue;
                }
                String function = feature[1];
                long window = parseTimestamp(feature[2]);
                JSONArray history = value.getJSONArray("HISTORY");
                String target = key.replace(".history", "").split("#")[0];
                Object featureResult;
                if ("sum".equalsIgnoreCase(function)) {
                    featureResult = doSum(history, target, window);
                } else if ("count".equalsIgnoreCase(function)) {
                    featureResult = doCount(history, target, window);
                } else if ("count_distinct".equalsIgnoreCase(function)) {
                    featureResult = doCountDistinct(history, target, window);
                } else {
                    throw new UnsupportedOperationException(String.format("unsupported function[%s]", function));
                }
                value.putIfAbsent("features", new JSONObject());
                String featureName = Joiner.on("&").join(feature);
                value.getJSONObject("features").put(featureName, featureResult);
            }
            return value;
        }

        private double doSum(JSONArray history, String target, long window) {
            long maxTimestamp = history.stream()
                    .map(x -> ((JSONObject) x).getLong("timestamp"))
                    .max(Long::compare).orElse(Long.MIN_VALUE);
            long minTimestamp = maxTimestamp - window;
            return history.stream()
                    .filter(x -> ((JSONObject) x).getLong("timestamp") > minTimestamp)
                    .map(x -> ((JSONObject) x).getDouble(target))
                    .reduce(Double::sum)
                    .orElse(Double.MIN_VALUE);
        }

        private double doCount(JSONArray history, String target, long window) {
            long maxTimestamp = history.stream()
                    .map(x -> ((JSONObject) x).getLong("timestamp"))
                    .max(Long::compare).orElse(Long.MIN_VALUE);
            long minTimestamp = maxTimestamp - window;
            return history.stream()
                    .filter(x -> ((JSONObject) x).getLong("timestamp") > minTimestamp)
                    .count();
        }

        private double doCountDistinct(JSONArray history, String target, long window) {
            long maxTimestamp = history.stream()
                    .map(x -> ((JSONObject) x).getLong("timestamp"))
                    .max(Long::compare).orElse(Long.MIN_VALUE);
            long minTimestamp = maxTimestamp - window;
            return history.stream()
                    .filter(x -> ((JSONObject) x).getLong("timestamp") > minTimestamp)
                    .map(x -> ((JSONObject) x).getString(target))
                    .distinct()
                    .count();
        }
    }

    public static long parseTimestamp(String time) {
        if (time.endsWith("d")) {
            return TimeUnit.DAYS.toMillis(Long.parseLong(time.replace("d", "")));
        } else if (time.endsWith("h")) {
            return TimeUnit.HOURS.toMillis(Long.parseLong(time.replace("h", "")));
        }
        throw new UnsupportedOperationException(String.format("unsupported time[%s]", time));
    }

    public static class FeatureReduceFunction extends RichFlatMapFunction<JSONObject, JSONObject> {

        private ValueState<JSONObject> merged;

        private static final List<String[]> features = FlinkFeatureExtractor.features2;

        @Override
        public void open(Configuration config) {
            merged = getRuntimeContext().getState(new ValueStateDescriptor<>("saved reduceJson", JSONObject.class));
        }

        @Override
        public void flatMap(JSONObject value, Collector<JSONObject> out) throws Exception {
            JSONObject mergedValue = merged.value();
            if (mergedValue == null) {
                mergedValue = new JSONObject();
            }

            String keyName = value.getString("KEY_NAME");
            if (keyName.equals("event")) {
                mergedValue.put("event", value);
            } else {
                mergedValue.putIfAbsent("features", new JSONObject());
                if (value.containsKey("features")) {
                    mergedValue.getJSONObject("features").putAll(value.getJSONObject("features"));
                }
            }

            if (mergedValue.containsKey("event") && mergedValue.containsKey("features")
                    && mergedValue.getJSONObject("features").size() == features.size()) {
                out.collect(mergedValue);
                merged.clear();
            } else {
                merged.update(mergedValue);
            }
        }
    }

    private static String genKeyValue(JSONObject event, String key) {
        if (!key.endsWith(".history")) {
            throw new UnsupportedOperationException("unsupported key type");
        }

        String[] splits = key.replace(".history", "").split("#");
        String keyValue;
        if (splits.length == 1) {
            String target = splits[0];
            keyValue = String.format("%s#%s.history", target, String.valueOf(event.get(target)));
        } else if (splits.length == 2) {
            String target = splits[0];
            String on = splits[1];
            keyValue = String.format("%s#%s.history", target, String.valueOf(event.get(on)));
        } else {
            throw new UnsupportedOperationException("unsupported key type");
        }
        return keyValue;
    }

    private static Set<String> genKeyFields(String key) {
        if (!key.endsWith(".history")) {
            throw new UnsupportedOperationException("unsupported key type");
        }

        String[] splits = key.replace(".history", "").split("#");
        return new HashSet<>(Arrays.asList(splits));
    }


    private static FlinkKafkaConsumer010<String> createKafkaConsumer() {
        Properties properties = new Properties();
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");
        properties.setProperty("enable.auto.commit", "true");
        properties.setProperty("auto.commit.interval.ms", "1000");
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("session.timeout.ms", "30000");

        FlinkKafkaConsumer010<String> consumer010 = new FlinkKafkaConsumer010<String>("event-input",
                new SimpleStringSchema(), properties);
        consumer010.setStartFromLatest();
        return consumer010;
    }
}
