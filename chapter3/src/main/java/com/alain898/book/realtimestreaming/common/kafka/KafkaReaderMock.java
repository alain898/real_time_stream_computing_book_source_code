package com.alain898.book.realtimestreaming.common.kafka;

import com.google.common.base.Charsets;

import java.util.Properties;
import java.util.concurrent.TimeUnit;


public class KafkaReaderMock extends KafkaReader {
    public KafkaReaderMock(String zookeeperConnect, String topic, String groupId, String offsetReset) {
        super(zookeeperConnect, groupId, topic, offsetReset);
    }

    public KafkaReaderMock(Properties props, String topic) {
        super(props, topic);
    }

    @Override
    protected void initKafkaStream() {
        return;
    }

    @Override
    public boolean hasNext() {
        try {
            TimeUnit.MILLISECONDS.sleep(10);
        } catch (InterruptedException e) {
            return false;
        }
        return true;
    }

    @Override
    public byte[] next() {
        final String value = "{\"user_id\": \"u200710918\", \"client_timestamp\": \"1524646221000\", \"event_type\": \"loan\", \"amount\": 1000 }";
        return value.getBytes(Charsets.UTF_8);
    }
}
