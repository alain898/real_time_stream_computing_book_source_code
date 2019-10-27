package com.alain898.book.realtimestreaming.chapter8.camel;


import com.alibaba.fastjson.JSONObject;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.impl.DefaultCamelContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;


public class CamelSwitchRouteExample {
    private static final Logger logger = LoggerFactory.getLogger(CamelSwitchRouteExample.class);

    public static void main(String[] args) throws Exception {
        CamelContext context = new DefaultCamelContext();
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("kafka:localhost:9092?topic=input_events2&groupId=CamelSwitchRouteExample&autoOffsetReset=latest&serializerClass=kafka.serializer.StringEncoder")
                        .process(new Processor() {
                            @Override
                            public void process(Exchange exchange) throws Exception {
                                System.out.println(String.format("get event[%s]", exchange.getIn().getBody(String.class)));
                                exchange.getIn().setHeader(KafkaConstants.KEY, UUID.randomUUID().toString());
                                exchange.getIn().setHeader("event_type", JSONObject.parseObject(exchange.getIn().getBody(String.class)).getString("event_type"));
                                exchange.getIn().removeHeader(KafkaConstants.TOPIC); // 必须删除KafkaConstants.TOPIC,否则Camel会根据这个值无限循环发送
                            }
                        })
                        .choice()
                        .when(header("event_type").isEqualTo("click"))
                        .to("kafka:localhost:9092?topic=click&requestRequiredAcks=-1")
                        .when(header("event_type").isEqualTo("activate"))
                        .to("kafka:localhost:9092?topic=activate&requestRequiredAcks=-1")
                        .otherwise()
                        .to("kafka:localhost:9092?topic=other&requestRequiredAcks=-1");
            }
        });
        context.start();
        Tools.waitForever();
        context.stop();
    }
}
