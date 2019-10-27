package com.alain898.book.realtimestreaming.chapter10.flinkcontrolstream;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * Created by alain on 19/8/21.
 */
public class FlinkControlStream {

    public static void testFlinkControlStream() throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(3);

        // control stream
        List<Tuple1<String>> control = new ArrayList<>();
        control.add(new Tuple1<>("BLUE"));
        control.add(new Tuple1<>("YELLOW"));
        DataStream<Tuple1<String>> controlStream = env.fromCollection(control);


        // data stream
        List<Tuple1<String>> data = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            data.add(new Tuple1<>("BLUE"));
            data.add(new Tuple1<>("YELLOW"));
            data.add(new Tuple1<>("WHITE"));
            data.add(new Tuple1<>("RED"));
            data.add(new Tuple1<>("BLUE"));
            data.add(new Tuple1<>("YELLOW"));
            data.add(new Tuple1<>("RED"));
        }
        DataStream<Tuple1<String>> dataStream = env.fromCollection(data).keyBy(0);

        DataStream<String> result = controlStream
                .broadcast()
                .connect(dataStream)
                .flatMap(new ColorCoFlatMap());
        result.print();
        env.execute();
    }

    private static final class ColorCoFlatMap
            implements CoFlatMapFunction<Tuple1<String>, Tuple1<String>, String> {
        HashSet blacklist = new HashSet();

        @Override
        public void flatMap1(Tuple1<String> control_value, Collector<String> out) {
            blacklist.add(control_value);
        }

        @Override
        public void flatMap2(Tuple1<String> data_value, Collector<String> out) {
            if (blacklist.contains(data_value)) {
                out.collect("invalid color " + data_value);
            } else {
                out.collect("valid color " + data_value);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        testFlinkControlStream();
    }

}
