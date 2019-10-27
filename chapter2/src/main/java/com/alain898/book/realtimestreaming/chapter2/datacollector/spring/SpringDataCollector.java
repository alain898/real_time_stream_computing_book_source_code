package com.alain898.book.realtimestreaming.chapter2.datacollector.spring;

import com.alain898.book.realtimestreaming.common.RestHelper;
import com.alain898.book.realtimestreaming.common.kafka.KafkaWriter;
import com.alain898.book.realtimestreaming.common.kafka.KafkaWriterMock;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * Created by alain on 18/4/26.
 */
@Controller
@EnableAutoConfiguration
public class SpringDataCollector {
    private static final Logger logger = LoggerFactory.getLogger(SpringDataCollector.class);

    private JSONObject doExtractCleanTransform(JSONObject event) {
        // TODO: 实现抽取、清洗、转化具体逻辑
        return event;
    }

    private final String kafkaBroker = "127.0.0.1:9092";
    private final String topic = "collector_event";
    private final KafkaWriter kafkaWriter = new KafkaWriterMock(kafkaBroker);

    @PostMapping(path = "/event", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
    @ResponseBody()
    public String uploadEvent(@RequestBody byte[] body) {
        logger.info(String.format("current thread[%s]", Thread.currentThread().toString()));

        if (logger.isDebugEnabled()) {
            logger.debug(String.format("request body[%s]", JSONObject.toJSONString(body)));
        }

        // step1: 对消息进行解码
        JSONObject bodyJson;
        try {
            bodyJson = JSONObject.parseObject(new String(body, Charsets.UTF_8));
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("request bodyJson[%s]", bodyJson));
            }
        } catch (Exception e) {
            logger.error(String.format("exception caught, body[%s]", JSONObject.toJSONString(body)), e);
            return RestHelper.genResponseString(400, "非法请求");
        }

        // step2: 对消息进行抽取、清洗、转化
        JSONObject normEvent;
        try {
            normEvent = doExtractCleanTransform(bodyJson);
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("normEvent[%s]", normEvent));
            }
        } catch (Exception e) {
            logger.error(String.format("exception caught, bodyJson[%s]", bodyJson), e);
            return RestHelper.genResponseString(500, "转化失败");
        }

        // step3: 将格式规整化的消息发到消息中间件kafka
        try {
            if (normEvent != null) {
                kafkaWriter.send(topic, normEvent.toJSONString().getBytes(Charsets.UTF_8));
            }
            return RestHelper.genResponseString(200, "ok");
        } catch (Exception e) {
            logger.error(String.format("exception caught, normEvent[%s]", normEvent), e);
            return RestHelper.genResponseString(500, "发送失败");
        }
    }

    public static void main(String[] args) throws Exception {
        SpringApplication.run(SpringDataCollector.class, args);
    }
}
