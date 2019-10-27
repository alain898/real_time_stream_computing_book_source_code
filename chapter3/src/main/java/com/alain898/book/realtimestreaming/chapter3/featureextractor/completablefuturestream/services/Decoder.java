package com.alain898.book.realtimestreaming.chapter3.featureextractor.completablefuturestream.services;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Decoder {
    private static final Logger logger = LoggerFactory.getLogger(Decoder.class);

    public JSONObject decode(byte[] event) {
        Preconditions.checkNotNull(event, "event is null");

        JSONObject eventJson = JSONObject.parseObject(new String(event, Charsets.UTF_8));
        if (logger.isDebugEnabled()) {
            logger.debug("decoder data: " + eventJson.toString());
        }
        return eventJson;
    }
}
