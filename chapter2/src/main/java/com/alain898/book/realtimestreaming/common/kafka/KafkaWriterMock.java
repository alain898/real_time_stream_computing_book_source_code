package com.alain898.book.realtimestreaming.common.kafka;

import java.util.Properties;

/**
 * Created by alain on 18/5/8.
 */
public class KafkaWriterMock extends KafkaWriter {
    public KafkaWriterMock(String kafkaBroker) {
        super(kafkaBroker);
    }

    public KafkaWriterMock(Properties props) {
        super(props);
    }

    @Override
    public void send(String topic, String key, byte[] message) {

    }

    @Override
    public void send(String topic, byte[] message) {
    }
}
