package com.alain898.book.realtimestreaming.chapter6.storm.streams;

import com.alain898.book.realtimestreaming.chapter6.storm.spout.DemoWordSpout;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.streams.Pair;
import org.apache.storm.streams.PairStream;
import org.apache.storm.streams.Stream;
import org.apache.storm.streams.StreamBuilder;
import org.apache.storm.streams.operations.Consumer;
import org.apache.storm.streams.operations.mappers.ValueMapper;
import org.apache.storm.testing.TestWordSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;


public class WordCountExample {

    public static void main(String[] args) throws Exception {

        StreamBuilder builder = new StreamBuilder();
        // A stream of words
        Stream<String> words = builder.newStream(new DemoWordSpout(), new ValueMapper<String>(0));

        // create a stream of (word, 1) pairs and aggregate the count
        PairStream<String, Long> wordCounts = words
                .mapToPair(w -> Pair.of(w, 1))
                .countByKey();

        // print to a file
        wordCounts.forEach(new WordCountExample.Print2FileConsumer());

        Config config = new Config();
        String topologyName = "test";
        if (args.length > 0) {
            topologyName = args[0];
        }
        config.setNumWorkers(1);
        StormSubmitter.submitTopologyWithProgressBar(topologyName, config, builder.build());
    }

    public static class Print2FileConsumer<T> implements Consumer<T> {
        private static final long serialVersionUID = 1L;
        public static Logger logger = LoggerFactory.getLogger(TestWordSpout.class);

        public Print2FileConsumer() {
        }

        public void appendToFile(Object line) {

            try {
                Files.write(Paths.get("/logs/console.log"),
                        String.valueOf(line + "\n").getBytes(),
                        StandardOpenOption.APPEND, StandardOpenOption.CREATE);
            } catch (IOException e) {
                logger.error(String.format("failed to write[%s]", String.valueOf(line)), e);
            }

        }

        @Override
        public void accept(T input) {
            appendToFile(input);
        }
    }
}
