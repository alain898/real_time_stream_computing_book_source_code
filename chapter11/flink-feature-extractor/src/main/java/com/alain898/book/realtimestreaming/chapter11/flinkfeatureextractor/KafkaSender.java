package com.alain898.book.realtimestreaming.chapter11.flinkfeatureextractor;


import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaSender {
    private static final Logger logger = LoggerFactory.getLogger(KafkaSender.class);

    private final KafkaProducer<String, String> producer;
    private final String topic;

    public KafkaSender(String brokers, String topic) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        this.producer = new KafkaProducer<>(properties);
        this.topic = topic;
    }

    public void send() {
        int samples = 1000000;
        for (int i = 0; i < samples; i++) {
            String application = "app001";
            long timestamp = System.currentTimeMillis();
            String event_type = "transaction";
            String user_id = String.format("user%d", RandomUtils.nextInt(0, 5));
            String device_id = String.format("device%d", RandomUtils.nextInt(0, 5));
            float amount = RandomUtils.nextInt(0, 100);
            String event = JSONObject.toJSONString(new Event(
                    application, timestamp, event_type, user_id, device_id, amount));
            producer.send(new ProducerRecord<>(this.topic, null, event));
            logger.info(String.format("send event[%s]", event));
            Tools.sleep(1000);
        }
    }


    public static void main(String args[]) {
        new KafkaSender("localhost:9092", "event-input").send();
    }
}