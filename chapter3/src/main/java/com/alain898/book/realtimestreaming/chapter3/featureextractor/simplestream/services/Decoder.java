package com.alain898.book.realtimestreaming.chapter3.featureextractor.simplestream.services;

import com.alain898.book.realtimestreaming.chapter3.featureextractor.common.services.Queue;
import com.alain898.book.realtimestreaming.chapter3.featureextractor.common.services.ServiceInterface;
import com.alain898.book.realtimestreaming.chapter3.featureextractor.common.services.SimpleStreamService;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


public class Decoder extends SimpleStreamService<byte[], JSONObject> {
    private static final Logger logger = LoggerFactory.getLogger(Decoder.class);


    private final List<JSONObject> outputsList = new ArrayList<>(1);

    public Decoder(String name, List<ServiceInterface> upstreams,
                   Queue<byte[]> inputQueue, Queue<JSONObject> outputQueue) {
        super(name, upstreams, Lists.newArrayList(inputQueue), Lists.newArrayList(outputQueue));
    }

    @Override
    protected List<JSONObject> process(List<byte[]> inputs) throws Exception {
        byte[] event = inputs.get(0);
        if (event == null) {
            return null;
        }

        try {
            JSONObject eventJson = JSONObject.parseObject(new String(event, Charsets.UTF_8));
            if (logger.isDebugEnabled()) {
                logger.debug("decoder data: " + eventJson.toString());
            }
            outputsList.clear();
            outputsList.add(eventJson);
            return outputsList;
        } catch (Exception e) {
            logger.error(String.format("failed to process event[%s]", JSONObject.toJSONString(event)));
            return null;
        }
    }
}


