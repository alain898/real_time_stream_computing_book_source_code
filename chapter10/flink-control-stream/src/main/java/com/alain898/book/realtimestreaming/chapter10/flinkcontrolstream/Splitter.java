package com.alain898.book.realtimestreaming.chapter10.flinkcontrolstream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Created by alain on 18/9/25.
 */
public class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
    @Override
    public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
        for (String word: sentence.split(" ")) {
            out.collect(new Tuple2<>(word, 1));
        }
    }
}