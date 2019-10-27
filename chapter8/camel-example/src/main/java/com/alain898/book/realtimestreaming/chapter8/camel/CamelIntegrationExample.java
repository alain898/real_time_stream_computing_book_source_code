package com.alain898.book.realtimestreaming.chapter8.camel;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.impl.DefaultCamelContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class CamelIntegrationExample {
    private static final Logger logger = LoggerFactory.getLogger(CamelIntegrationExample.class);

    public static void main(String args[]) throws Exception {
        CamelContext context = new DefaultCamelContext();
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("quartz://timer001?trigger.repeatInterval=1000&trigger.repeatCount=-1")
                        .to("http://localhost:8080/hello")
                        .convertBodyTo(String.class)
                        .process(new Processor() {
                            @Override
                            public void process(Exchange exchange) throws Exception {
                                // 为消息设置一个用于分区的key
                                String key = UUID.randomUUID().toString();
                                System.out.println("key: " + key);
                                exchange.getIn().setHeader(KafkaConstants.KEY, key);
                            }
                        })
                        .to("kafka:localhost:9092?topic=kafka-example&requestRequiredAcks=-1");
            }
        });
        context.start();
        Tools.waitForever();
        context.stop();
    }
}
