package com.alain898.book.realtimestreaming.chapter7.kappa.example;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.StringUtils;

import java.util.HashMap;
import java.util.Map;

public class FastLayer {
    public static final Time FAST_LAYER_WINDOW = Time.seconds(15);
    public static final Time FAST_LAYER_SLIDE = FAST_LAYER_WINDOW;
    public static final long FAST_LAYER_SLIDES_IN_WINDOW =
            FAST_LAYER_WINDOW.toMilliseconds() / FAST_LAYER_SLIDE.toMilliseconds();


    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(5000);

        Map<String, String> properties = new HashMap<>();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("group.id", "FastLayer");
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
                .keyBy(x -> x.product)
                // use TumblingEventTimeWindows for FastLayer
                .window(TumblingEventTimeWindows.of(FAST_LAYER_WINDOW))
                .reduce((e1, e2) -> {
                    CountedEvent countedEvent = new CountedEvent();
                    countedEvent.product = e1.product;
                    countedEvent.timestamp = e1.timestamp;
                    countedEvent.count = e1.count + e2.count;
                    countedEvent.minTimestamp = Math.min(e1.minTimestamp, e2.minTimestamp);
                    countedEvent.maxTimestamp = Math.min(e1.maxTimestamp, e2.maxTimestamp);
                    return countedEvent;
                });

        counts.addSink(new JdbcWriter(FAST_LAYER_SLIDE.toMilliseconds(), FAST_LAYER_SLIDES_IN_WINDOW, "fast"));
        counts.print().setParallelism(1);
        env.execute("product count from Kafka data");

    }

}
