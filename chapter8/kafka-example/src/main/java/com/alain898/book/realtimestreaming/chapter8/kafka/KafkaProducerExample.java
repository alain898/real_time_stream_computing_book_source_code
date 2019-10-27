package com.alain898.book.realtimestreaming.chapter8.kafka;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaProducerExample {
    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerExample.class);

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("compression.type", "gzip");
        props.put("retries", 1);
        props.put("max.in.flight.requests.per.connection", 2);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<>(props);

        String topic = "kafka-example";

        int samples = 1000000;
        int productNumber = 5;
        for (int i = 0; i < samples; i++) {
            String productId = String.format("product_%d", RandomUtils.nextInt(0, productNumber));
            String event = JSONObject.toJSONString(new Event(productId, System.currentTimeMillis()));
            Future<RecordMetadata> future = producer.send(new ProducerRecord<>(topic, productId, event),
                    new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception exception) {
                            if (exception == null) {
                                logger.info(String.format("succeed to send event[%s]", event));
                            } else {
                                logger.error(String.format("failed to send event[%s]", event));
                            }
                        }
                    });
//            future.get() // 同步发送方式
            Tools.sleep(1000);
        }
    }
}
