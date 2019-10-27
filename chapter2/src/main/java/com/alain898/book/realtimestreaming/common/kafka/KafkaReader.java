package com.alain898.book.realtimestreaming.common.kafka;

import com.google.common.base.Preconditions;
import kafka.consumer.*;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by alain on 15/11/20.
 */
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
        List<String> list =new ArrayList<>();
        list.stream().count();
    }

    public KafkaReader(final String zookeeperConnect, String groupId, String offsetReset, String topic) {
        Preconditions.checkNotNull(zookeeperConnect, "zookeeperConnect is null");
        Preconditions.checkNotNull(topic, "topic is null");
        Preconditions.checkNotNull(groupId, "groupId is null");
        Preconditions.checkNotNull(offsetReset, "offsetReset is null");

        Properties kafkaProp = new Properties();
        kafkaProp.setProperty("zookeeper.connect", zookeeperConnect);
        kafkaProp.setProperty("group.id", groupId);
        kafkaProp.setProperty("auto.offset.reset", offsetReset);
        this.props = PropertiesUtil.newProperties(DEFAULT_KAFKA_PROPERTIES, kafkaProp);
        this.topic = topic;
        this.consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(this.props));
        initKafkaStream();
    }

    public KafkaReader(final Properties props, final String topic) {
        Preconditions.checkNotNull(props, "props is null");
        Preconditions.checkNotNull(topic, "topic is null");

        this.props = PropertiesUtil.newProperties(DEFAULT_KAFKA_PROPERTIES, props);
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
}

