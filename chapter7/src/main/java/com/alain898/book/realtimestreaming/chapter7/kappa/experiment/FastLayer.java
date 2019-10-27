package com.alain898.book.realtimestreaming.chapter7.kappa.experiment;

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


        DataStream<String> stream = env
                .addSource(myConsumer);


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
                        return new CountedEvent(event.product, event.timestamp,
                                CountedEvent.FAST_LAYER_SLIDE_STEP_IN_SECONDS);
                    }
                })
                .assignTimestampsAndWatermarks(new EventTimestampPeriodicWatermarks())
                .keyBy(x -> x.product)
                // use TumblingEventTimeWindows for FastLayer
                .window(TumblingEventTimeWindows.of(Time.seconds(CountedEvent.FAST_LAYER_SLIDE_STEP_IN_SECONDS)))
                .reduce((e1, e2) -> {
                    CountedEvent countedEvent = new CountedEvent();
                    countedEvent.product = e1.product;
                    countedEvent.timestamp = e1.timestamp;
                    countedEvent._times.addAll(e1._times);
                    countedEvent._times.addAll(e2._times);// 滑动窗口是连续的多个窗口id，取其中最小的就是窗口起始时间
                    countedEvent._count = e1._count + e2._count;
                    countedEvent._minTimestamp = Math.min(e1._minTimestamp, e2._minTimestamp);
                    return countedEvent;
                });


        counts.print().setParallelism(1);
        env.execute("product count from Kafka data");

    }

}
