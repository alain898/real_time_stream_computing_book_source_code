package com.alain898.book.realtimestreaming.chapter8.rabbitmq;


import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Charsets;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.commons.lang3.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class RabbitMQProducerExample {
    private static final Logger logger = LoggerFactory.getLogger(RabbitMQProducerExample.class);

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(5672);
        factory.setUsername("admin");
        factory.setPassword("admin");
        factory.setVirtualHost("/"); //可以从'http://127.0.0.1:15672/#/vhosts'查看到vhost的名字

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        String exchangeName = "exchange001";
        channel.exchangeDeclare(exchangeName, BuiltinExchangeType.DIRECT,
                true, false, false, new HashMap<>());
        String queueName = "queue001";
        channel.queueDeclare(queueName, true, false, false, null);

        String routingKey = "routingkey001";
        channel.queueBind(queueName, exchangeName, routingKey);

        int samples = 1000000;
        int productNumber = 5;
        for (int i = 0; i < samples; i++) {
            String productId = String.format("product_%d", RandomUtils.nextInt(0, productNumber));
            String event = JSONObject.toJSONString(new Event(productId, System.currentTimeMillis()));
            channel.basicPublish(exchangeName, routingKey, null, event.getBytes(Charsets.UTF_8));
            logger.info(String.format("send event[%s]", event));
            Tools.sleep(1000);
        }

    }
}
