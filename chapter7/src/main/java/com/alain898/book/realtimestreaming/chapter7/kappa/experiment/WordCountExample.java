package com.alain898.book.realtimestreaming.chapter7.kappa.experiment;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * Created by alain on 19/8/21.
 */
public class WordCountExample {


    public static void main(String[] args) throws Exception {


        // get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // get input data by connecting to the socket
        DataStream<String> text = env.socketTextStream("localhost", 9999, "\n");

        // parse the data, group it, window it, and aggregate the counts
        DataStream<Event> windowCounts = text
                .flatMap(new FlatMapFunction<String, Event>() {
                    @Override
                    public void flatMap(String value, Collector<Event> out) {
                        for (String word : value.split("\\s")) {
                            out.collect(new Event(word, 1L));
                        }
                    }
                })
                .keyBy("product")
                .timeWindow(Time.seconds(5), Time.seconds(1))
                .reduce(new ReduceFunction<Event>() {
                    @Override
                    public Event reduce(Event a, Event b) {
                        return new Event(a.product, a.timestamp + b.timestamp);
                    }
                });

        // print the results with a single thread, rather than in parallel
        windowCounts.print().setParallelism(1);

        env.execute("Socket Window WordCount");
    }



}
