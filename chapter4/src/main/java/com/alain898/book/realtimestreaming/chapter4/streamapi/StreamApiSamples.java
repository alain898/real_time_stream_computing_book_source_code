package com.alain898.book.realtimestreaming.chapter4.streamapi;

import com.alain898.book.realtimestreaming.common.TimeTools;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by alain on 19/8/21.
 */
public class StreamApiSamples {

    public static void filterSample() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<JSONObject> temperatureStream = env
                .addSource(new PeriodicTemperatureSensorSourceFunction())
                .assignTimestampsAndWatermarks(new StreamApiSamples.EventTimestampPeriodicWatermarks())
                .setParallelism(1);

        DataStream<JSONObject> highTemperatureStream = temperatureStream.filter(x -> x.getDouble("temperature") > 100);

        highTemperatureStream.print().setParallelism(1);
        env.execute();
    }

    public static void mapSample() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<JSONObject> temperatureStream = env
                .addSource(new PeriodicTemperatureSensorSourceFunction())
                .assignTimestampsAndWatermarks(new StreamApiSamples.EventTimestampPeriodicWatermarks())
                .setParallelism(1);

        DataStream<JSONObject> enhancedTemperatureStream = temperatureStream.map(x -> {
            x.put("isHighTemperature", x.getDouble("temperature") > 100);
            return x;
        });

        enhancedTemperatureStream.print().setParallelism(1);
        env.execute();
    }

    public static void flatMapSample() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<JSONObject> socialWebStream = env
                .addSource(new PeriodicSocialWebSourceFunction())
                .assignTimestampsAndWatermarks(new StreamApiSamples.EventTimestampPeriodicWatermarks())
                .setParallelism(1);

        DataStream<String> relationStream = socialWebStream.flatMap(new FlatMapFunction<JSONObject, String>() {
            @Override
            public void flatMap(JSONObject value, Collector<String> out) throws Exception {
                List<String> collect = value.getJSONArray("friends").stream()
                        .map(y -> String.format("%s->%s", value.getString("user"), y))
                        .collect(Collectors.toList());
                collect.forEach(out::collect);
            }
        });

        relationStream.print().setParallelism(1);
        env.execute();
    }

    public static void reduceSample() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<JSONObject> socialWebStream = env
                .addSource(new PeriodicSocialWebSourceFunction())
                .assignTimestampsAndWatermarks(new StreamApiSamples.EventTimestampPeriodicWatermarks())
                .setParallelism(1);

        DataStream<Tuple2<String, Integer>> countStream = socialWebStream
                .map(x -> Tuple2.of("count", 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .timeWindowAll(Time.seconds(10), Time.seconds(1))
                .reduce((count1, count2) -> Tuple2.of("count", count1.f1 + count2.f1));

        countStream.print().setParallelism(1);
        env.execute();
    }

    public static void joinSample() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<JSONObject> socialWebStream = env
                .addSource(new PeriodicSocialWebSourceFunction())
                .assignTimestampsAndWatermarks(new StreamApiSamples.EventTimestampPeriodicWatermarks())
                .setParallelism(1);

        DataStream<JSONObject> socialWebStream2 = env
                .addSource(new PeriodicSocialWeb2SourceFunction())
                .assignTimestampsAndWatermarks(new StreamApiSamples.EventTimestampPeriodicWatermarks())
                .setParallelism(1);

        DataStream<JSONObject> joinStream = socialWebStream.join(socialWebStream2)
                .where(x1 -> x1.getString("user"))
                .equalTo(x2 -> x2.getString("user"))
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply((x1, x2) -> {
                    JSONObject res = new JSONObject();
                    res.putAll(x1);
                    res.putAll(x2);
                    return res;
                });

        joinStream.print().setParallelism(1);
        env.execute();
    }


    public static void groupBySample() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<JSONObject> transactionStream = env
                .addSource(new PeriodicTransactionSourceFunction())
                .assignTimestampsAndWatermarks(new StreamApiSamples.EventTimestampPeriodicWatermarks())
                .setParallelism(1);

        DataStream<Tuple2<String, Integer>> keyedStream = transactionStream
                .map(x -> Tuple2.of(x.getString("product"), x.getInteger("number")))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .sum(1);

        keyedStream.print().setParallelism(1);
        env.execute();
    }

    public static void foreachSample() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<JSONObject> transactionStream = env
                .addSource(new PeriodicTransactionSourceFunction())
                .assignTimestampsAndWatermarks(new StreamApiSamples.EventTimestampPeriodicWatermarks())
                .setParallelism(1);

        transactionStream.addSink(new PrintSinkFunction<>()).name("Print to Std. Out");
        env.execute();
    }

    public static void main(String[] args) throws Exception {
//        filterSample();
//        mapSample();
//        flatMapSample();
//        reduceSample();
//        joinSample();
        groupBySample();
    }

    private static class PeriodicTemperatureSensorSourceFunction extends RichSourceFunction<JSONObject> {

        private final AtomicLong id = new AtomicLong();
        private volatile boolean running = true;

        PeriodicTemperatureSensorSourceFunction() {
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

    private static class PeriodicSocialWebSourceFunction extends RichSourceFunction<JSONObject> {

        private final AtomicLong id = new AtomicLong();
        private volatile boolean running = true;

        PeriodicSocialWebSourceFunction() {
        }

        public void run(SourceContext<JSONObject> ctx) throws Exception {
            while (running) {
                TimeTools.sleepSec(1);
                ctx.collect(buildEvent());
            }
        }

        private String genUserId() {
            int userId = RandomUtils.nextInt(0, 10);
            return String.format("user_%03d", userId);
        }

        private JSONObject buildEvent() {
            JSONObject ret = new JSONObject();
            ret.put("id", id.getAndIncrement());
            ret.put("timestamp", System.currentTimeMillis());
            ret.put("user", genUserId());
            ret.put("friends", IntStream.range(1, RandomUtils.nextInt(2, 10)).mapToObj(x -> genUserId()).collect(Collectors.toList()));
            return ret;
        }

        public void cancel() {
            running = false;
        }
    }


    private static class PeriodicSocialWeb2SourceFunction extends RichSourceFunction<JSONObject> {

        private final AtomicLong id = new AtomicLong();
        private volatile boolean running = true;

        PeriodicSocialWeb2SourceFunction() {
        }

        public void run(SourceContext<JSONObject> ctx) throws Exception {
            while (running) {
                TimeTools.sleepSec(1);
                ctx.collect(buildEvent());
            }
        }

        private String genUserId() {
            int userId = RandomUtils.nextInt(0, 10);
            return String.format("user_%03d", userId);
        }

        private String genHobby() {
            int userId = RandomUtils.nextInt(0, 10);
            return String.format("hobby_%03d", userId);
        }

        private JSONObject buildEvent() {
            JSONObject ret = new JSONObject();
            ret.put("id", id.getAndIncrement());
            ret.put("timestamp", System.currentTimeMillis());
            ret.put("user", genUserId());
            ret.put("hobbies", IntStream.range(1, RandomUtils.nextInt(2, 10)).mapToObj(x -> genHobby()).collect(Collectors.toList()));
            return ret;
        }

        public void cancel() {
            running = false;
        }
    }


    private static class PeriodicTransactionSourceFunction extends RichSourceFunction<JSONObject> {

        private final AtomicLong id = new AtomicLong();
        private volatile boolean running = true;

        PeriodicTransactionSourceFunction() {
        }

        public void run(SourceContext<JSONObject> ctx) throws Exception {
            while (running) {
                TimeTools.sleepSec(1);
                ctx.collect(buildEvent());
            }
        }

        private int genNumber() {
            return RandomUtils.nextInt(0, 5);
        }

        private String genProduct() {
            String[] products = {"Huawei", "Oppo", "Xiaomi", "Honor", "Apple"};
            int index = RandomUtils.nextInt(0, products.length);
            return products[index];
        }

        private JSONObject buildEvent() {
            JSONObject ret = new JSONObject();
            ret.put("id", id.getAndIncrement());
            ret.put("timestamp", System.currentTimeMillis());
            ret.put("product", genProduct());
            ret.put("number", genNumber());
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
