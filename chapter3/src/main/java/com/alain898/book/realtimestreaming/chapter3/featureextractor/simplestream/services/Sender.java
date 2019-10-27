package com.alain898.book.realtimestreaming.chapter3.featureextractor.simplestream.services;

import com.alain898.book.realtimestreaming.chapter3.featureextractor.common.services.Queue;
import com.alain898.book.realtimestreaming.chapter3.featureextractor.common.services.ServiceInterface;
import com.alain898.book.realtimestreaming.chapter3.featureextractor.common.services.SimpleStreamService;
import com.alain898.book.realtimestreaming.common.kafka.KafkaWriter;
import com.alain898.book.realtimestreaming.common.kafka.KafkaWriterMock;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.List;


public class Sender extends SimpleStreamService<JSONObject, JSONObject> {
    private static final Logger logger = LoggerFactory.getLogger(Sender.class);

    private final String kafkaBroker = "127.0.0.1:9092";
    private final String topic = "collector_event";
    private final KafkaWriter kafkaWriter = new KafkaWriterMock(kafkaBroker);


    public Sender(String name, List<ServiceInterface> upstreams, Queue<JSONObject> inputQueue) {
        super(name, upstreams, Lists.newArrayList(inputQueue), null);
    }

    @Override
    protected List<JSONObject> process(List<JSONObject> inputs) throws Exception {
        return inputs;
    }

    @Override
    protected boolean offer(List<Queue<JSONObject>> outputQueues, List<JSONObject> outputs) throws Exception {
        JSONObject event = outputs.get(0);
        if (event == null) {
            return true;
        }
        kafkaWriter.send(topic, event.toString().getBytes(Charset.forName("UTF-8")));
        return true;
    }
}

