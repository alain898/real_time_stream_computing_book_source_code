package com.alain898.book.realtimestreaming.common.kafka;

import com.google.common.base.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class KafkaWriterMock extends KafkaWriter {
    private static final Logger logger = LoggerFactory.getLogger(KafkaWriterMock.class);

    public KafkaWriterMock(String kafkaBroker) {
        super(kafkaBroker);
    }

    public KafkaWriterMock(Properties props) {
        super(props);
    }

    @Override
    public void send(String topic, String key, byte[] message) {
        logger.info(String.format("send topic[%s] key[%s] message[%s]",
                topic, key, new String(message, Charsets.UTF_8)));
    }

    @Override
    public void send(String topic, byte[] message) {
        logger.info(String.format("send topic[%s] message[%s]",
                topic, new String(message, Charsets.UTF_8)));
    }
}
