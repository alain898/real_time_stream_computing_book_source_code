package com.alain898.book.realtimestreaming.chapter8.rabbitmq;

import com.google.common.base.Charsets;
import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;

public class RabbitMQConsumerExample {
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

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                                       byte[] body) throws IOException {
                String event = new String(body, Charsets.UTF_8);
                logger.info(String.format("receive exchange[%s], routingKey[%s], event[%s]",
                        envelope.getExchange(), envelope.getRoutingKey(), event));
            }
        };
        channel.basicConsume(queueName, true, consumer);
    }
}
