package com.alain898.book.realtimestreaming.common.kafka;

import com.google.common.base.Preconditions;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;


public class KafkaWriter {

    private Properties props;
    private Producer<String, byte[]> producer;

    final static protected Properties DEFAULT_KAFKA_PROPERTIES = new Properties();

    static {
        DEFAULT_KAFKA_PROPERTIES.put("metadata.broker.list", "127.0.0.1:9092");
        DEFAULT_KAFKA_PROPERTIES.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        //0, which means that the producer never waits for an acknowledgement from the broker (the same behavior as 0.7). This option provides the lowest latency but the weakest durability guarantees (some data will be lost when a server fails).
        //1, which means that the producer gets an acknowledgement after the leader replica has received the data. This option provides better durability as the client waits until the server acknowledges the request as successful (must messages that were written to the now-dead leader but not yet replicated will be lost).
        //-1, which means that the producer gets an acknowledgement after all in-sync replicas have received the data. This option provides the best durability, we guarantee that no messages will be lost as long as at least one in sync replica remains.
        DEFAULT_KAFKA_PROPERTIES.put("request.required.acks", "1");
        DEFAULT_KAFKA_PROPERTIES.put("partitioner.class", "com.alain898.book.realtimestreaming.common.kafka.WriterPartitioner");
        DEFAULT_KAFKA_PROPERTIES.put("key.serializer.class", "kafka.serializer.StringEncoder");
//        DEFAULT_KAFKA_PROPERTIES.put("producer.type", "async");
//        DEFAULT_KAFKA_PROPERTIES.put("batch.num.messages", "100");
    }

    public KafkaWriter(final String kafkaBroker) {
        Preconditions.checkNotNull(kafkaBroker, "kafkaBroker is null");

        Properties kafkaProp = new Properties();
        kafkaProp.put("metadata.broker.list", kafkaBroker);
        this.props = PropertiesUtil.newProperties(DEFAULT_KAFKA_PROPERTIES, kafkaProp);
        this.producer = new Producer<>(new ProducerConfig(this.props));
    }

    public KafkaWriter(final Properties props) {
        Preconditions.checkNotNull(props, "props is null");

        this.props = PropertiesUtil.newProperties(DEFAULT_KAFKA_PROPERTIES, props);
        this.producer = new Producer<>(new ProducerConfig(this.props));
    }

    public void send(String topic, String key, byte[] message) {
        producer.send(new KeyedMessage<>(topic, key, message));
    }

    public void send(String topic, byte[] message) {
        this.send(topic, null, message);
    }
}

