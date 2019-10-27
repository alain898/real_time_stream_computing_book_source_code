package com.alain898.book.realtimestreaming.chapter3.featureextractor.simplestream.services;


import com.alain898.book.realtimestreaming.chapter3.featureextractor.common.services.Queue;
import com.alain898.book.realtimestreaming.chapter3.featureextractor.common.services.SimpleStreamService;
import com.alain898.book.realtimestreaming.common.kafka.KafkaReader;
import com.alain898.book.realtimestreaming.common.kafka.KafkaReaderMock;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;


public class Receiver extends SimpleStreamService<byte[], byte[]> {

    private final List<byte[]> inputsList = new ArrayList<>(1);

    private final KafkaReader kafkaReader = new KafkaReaderMock(
            "127.0.0.1", "collector_event",
            "feature_extractor", "largest");

    public Receiver(String name, Queue<byte[]> outputQueue) {
        super(name, null, null, Lists.newArrayList(outputQueue));
    }

    @Override
    protected List<byte[]> poll(List<Queue<byte[]>> inputQueues) throws Exception {
        if (stopped) return null;

        inputsList.clear();
        if (kafkaReader.hasNext()) {
            byte[] event = kafkaReader.next();

            inputsList.add(event);
            return inputsList;
        } else {
            return null;
        }
    }

    @Override
    protected List<byte[]> process(List<byte[]> inputs) throws Exception {
        TimeUnit.SECONDS.sleep(1);
        return inputs;
    }
}
