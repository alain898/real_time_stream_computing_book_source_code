package com.alain898.book.realtimestreaming.chapter3.featureextractor.completablefuturestream.services;

import com.alain898.book.realtimestreaming.common.kafka.KafkaWriter;
import com.alain898.book.realtimestreaming.common.kafka.KafkaWriterMock;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;


public class Sender {
    private static final Logger logger = LoggerFactory.getLogger(Sender.class);

    private final String kafkaBroker = "127.0.0.1:9092";
    private final String topic = "collector_event";

    // TODO: create a sender for each thread
    private final KafkaWriter kafkaWriter = new KafkaWriterMock(kafkaBroker);

    public void send(JSONObject event) {
        Preconditions.checkNotNull(event, "event is null");

        kafkaWriter.send(topic, event.toString().getBytes(Charset.forName("UTF-8")));
    }
}
