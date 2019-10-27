package com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.tools;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

public class KafkaWriter {

    private Properties props;
    private Producer<String, byte[]> producer;
    private String topic;

    final static protected Properties DEFAULT_KAFKA_PROPERTIES = new Properties();

    static {
        DEFAULT_KAFKA_PROPERTIES.put("metadata.broker.list", "127.0.0.1:9092");
        DEFAULT_KAFKA_PROPERTIES.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        DEFAULT_KAFKA_PROPERTIES.put("request.required.acks", "1");
        DEFAULT_KAFKA_PROPERTIES.put("key.serializer.class", "kafka.serializer.StringEncoder");
        DEFAULT_KAFKA_PROPERTIES.put("producer.type", "async");
        DEFAULT_KAFKA_PROPERTIES.put("batch.num.messages", "100");
    }

    public KafkaWriter(Properties props, String topic) {
        this.props = PropertiesUtils.newProperties(DEFAULT_KAFKA_PROPERTIES, props);
        this.producer = new Producer<>(new ProducerConfig(this.props));
        this.topic = topic;
    }

    public void send(String key, byte[] message) {
        producer.send(new KeyedMessage<>(topic, key, message));
    }

    public void send(byte[] message) {
        send(null, message);
    }

    public void close() {
        producer.close();
    }
}

