package com.alain898.book.realtimestreaming.chapter8.camel;

import com.alibaba.fastjson.JSONObject;
import org.apache.camel.Exchange;
import org.apache.camel.component.kafka.KafkaConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DynamicRouter {
    private static final Logger logger = LoggerFactory.getLogger(DynamicRouter.class);

    public String slip(Exchange exchange) {
        try {
            Integer currentEndpointIndex = exchange.getProperty("currentEndpointIndex", Integer.class);
            if (currentEndpointIndex == null) {
                currentEndpointIndex = 0;
            }
            exchange.setProperty("currentEndpointIndex", currentEndpointIndex + 1);

            String eventType = exchange.getIn().getHeader("event_type", String.class);
            if (StringUtils.isEmpty(eventType)) {
                return null;
            }

            List<String> endpoints = getEndpoints(eventType);
            if (CollectionUtils.isEmpty(endpoints)) {
                return null;
            }

            if (currentEndpointIndex >= endpoints.size()) {
                return null;
            }

            String endpoint = endpoints.get(currentEndpointIndex);
            String topic = parseTopicFromEndpoint(endpoint);
            exchange.getIn().setHeader(KafkaConstants.TOPIC, topic);
            logger.info("send event[%s] to endpoint[%s]", exchange.getProperty("eventId"), endpoint);
            return endpoint;
        } catch (Exception e) {
            logger.error(String.format("exception when slip exchange[%s]", JSONObject.toJSONString(exchange)), e);
            return null;
        }
    }

    private List<String> getEndpoints(String eventType) {
        Map<String, List<String>> eventEndpoints = new HashMap<>();
        eventEndpoints.put("click", Arrays.asList(
                "kafka:localhost:9092?topic=subsystem1&requestRequiredAcks=-1",
                "kafka:localhost:9092?topic=subsystem2&requestRequiredAcks=-1"));
        eventEndpoints.put("activate", Arrays.asList(
                "kafka:localhost:9092?topic=subsystem1&requestRequiredAcks=-1",
                "kafka:localhost:9092?topic=subsystem2&requestRequiredAcks=-1",
                "kafka:localhost:9092?topic=subsystem3&requestRequiredAcks=-1"));
        eventEndpoints.put("other", Arrays.asList(
                "kafka:localhost:9092?topic=subsystem2&requestRequiredAcks=-1",
                "kafka:localhost:9092?topic=subsystem3&requestRequiredAcks=-1"));
        return eventEndpoints.get(eventType);
    }

    private String parseTopicFromEndpoint(String endpoint) {
        String[] params = endpoint.split("\\?")[1].split("&");
        for (String param : params) {
            String[] splits = param.split("=");
            if ("topic".equals(splits[0])) {
                return splits[1];
            }
        }
        return null;
    }
}