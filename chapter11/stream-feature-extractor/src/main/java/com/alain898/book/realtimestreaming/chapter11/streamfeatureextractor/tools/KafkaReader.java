package com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.tools;

import kafka.consumer.*;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaReader {
    private static final Logger logger = LoggerFactory.getLogger(KafkaReader.class);

    protected Properties props;
    protected String topic;
    protected ConsumerConnector consumer;
    private ConsumerIterator<byte[], byte[]> consumerIterator;

    final static protected Properties DEFAULT_KAFKA_PROPERTIES = new Properties();

    static {
        DEFAULT_KAFKA_PROPERTIES.put("zookeeper.connect", "127.0.0.1:2181");
        DEFAULT_KAFKA_PROPERTIES.put("zookeeper.session.timeout.ms", "4000");
        DEFAULT_KAFKA_PROPERTIES.put("zookeeper.sync.time.ms", "2000");
        DEFAULT_KAFKA_PROPERTIES.put("auto.commit.interval.ms", "1000");
        DEFAULT_KAFKA_PROPERTIES.put("auto.offset.reset", "largest");
    }

    public KafkaReader(final Properties props, final String topic) {
        this.props = PropertiesUtils.newProperties(DEFAULT_KAFKA_PROPERTIES, props);
        this.topic = topic;
        this.consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(this.props));
        initKafkaStream();
    }

    private void initKafkaStream() {
        HashMap<String, Integer> topicCountMap = new HashMap<>();
        // use must one thread to preserve event order.
        topicCountMap.put(topic, 1);
        logger.info(String.format("initKafkaStream with topic[%s]", topic));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> kafkaStream = consumerMap.get(topic).get(0);
        this.consumerIterator = kafkaStream.iterator();
    }

    public boolean hasNext() {
        try {
            consumerIterator.hasNext();
            return true;
        } catch (ConsumerTimeoutException ex) {
//            logger.info("kafka receiver timeout.");
            return false;
        } catch (Exception ex) {
            logger.error("kafka receiver exception: " + ex);
            throw ex;
        }
    }

    public byte[] next() {
        MessageAndMetadata<byte[], byte[]> msg = consumerIterator.next();
        return msg.message();
    }

    public void close() {
        consumer.shutdown();
    }
}

