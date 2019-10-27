package com.alain898.book.realtimestreaming.chapter7.kappa.example;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.StringUtils;

import java.util.HashMap;
import java.util.Map;

public class BatchLayer {
    //    public static final Time BATCH_LAYER_WINDOW = Time.days(3);
//    public static final Time BATCH_LAYER_SLIDE = Time.minutes(30);
    public static final Time BATCH_LAYER_WINDOW = Time.minutes(3); // for test
    public static final Time BATCH_LAYER_SLIDE = Time.minutes(1); // fot test
    public static final long BATCH_LAYER_SLIDES_IN_WINDOW =
            BATCH_LAYER_WINDOW.toMilliseconds() / BATCH_LAYER_SLIDE.toMilliseconds();


    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(1000);

        Map<String, String> properties = new HashMap<>();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("group.id", "BatchLayer");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("auto.offset.reset", "earliest");
        properties.put("session.timeout.ms", "30000");
//        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        properties.put("product.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        ParameterTool parameterTool = ParameterTool.fromMap(properties);

        FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<String>(
                "event-input", new SimpleStringSchema(), parameterTool.getProperties());


        DataStream<String> stream = env.addSource(myConsumer);


        DataStream counts = stream
                .map(new MapFunction<String, Event>() {
                    @Override
                    public Event map(String s) throws Exception {
                        if (StringUtils.isNullOrWhitespaceOnly(s)) {
                            return null;
                        }
                        return JSONObject.parseObject(s, Event.class);
                    }
                })
                .map(new MapFunction<Event, CountedEvent>() {
                    @Override
                    public CountedEvent map(Event event) throws Exception {
                        return new CountedEvent(event.product, 1, event.timestamp);
                    }
                })
                .assignTimestampsAndWatermarks(new EventTimestampPeriodicWatermarks())
                .keyBy("product")
                // use SlidingEventTimeWindows for BatchLayer
                .timeWindow(Time.minutes(3), Time.minutes(1)) //使用较短的窗口进行演示
//                .timeWindow(Time.days(3), Time.minutes(30))
                .reduce((e1, e2) -> {
                    CountedEvent countedEvent = new CountedEvent();
                    countedEvent.product = e1.product;
                    countedEvent.timestamp = e1.timestamp;
                    countedEvent.count = e1.count + e2.count;
                    countedEvent.minTimestamp = Math.min(e1.minTimestamp, e2.minTimestamp);
                    countedEvent.maxTimestamp = Math.min(e1.maxTimestamp, e2.maxTimestamp);
                    return countedEvent;
                });
        counts.addSink(new JdbcWriter(BATCH_LAYER_SLIDE.toMilliseconds(), BATCH_LAYER_SLIDES_IN_WINDOW, "batch"));
        counts.print().setParallelism(1);
        env.execute("product count from Kafka data");
    }


}
