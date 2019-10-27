package com.alain898.book.realtimestreaming.chapter3.featureextractor.completablefuturestream.services;

import com.alain898.book.realtimestreaming.common.TimeTools;
import com.alain898.book.realtimestreaming.common.kafka.KafkaReader;
import com.alain898.book.realtimestreaming.common.kafka.KafkaReaderMock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Receiver {
    private static final Logger logger = LoggerFactory.getLogger(Receiver.class);

    private final KafkaReader kafkaReader = new KafkaReaderMock(
            "127.0.0.1", "collector_event",
            "feature_extractor", "largest");


    public byte[] receive() {
        if (kafkaReader.hasNext()) {
            TimeTools.sleepSec(1);
            return kafkaReader.next();
        } else {
            TimeTools.sleepMS(100);
            return null;
        }
    }

}
