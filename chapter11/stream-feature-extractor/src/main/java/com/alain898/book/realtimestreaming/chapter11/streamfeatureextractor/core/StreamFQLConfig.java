package com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.core;

import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamFQLConfig {
    private static final Logger logger = LoggerFactory.getLogger(StreamFQLConfig.class);

    private static volatile JSONObject CONFIG;


    public static JSONObject getConfig() {
        if (CONFIG != null) {
            return CONFIG;
        }
        synchronized (StreamFQLConfig.class) {
            if (CONFIG != null) {
                return CONFIG;
            }
            CONFIG = new JSONObject();
            return CONFIG;
        }
    }

    public static JSONObject duplicateConfig() {
        return copyConfig(getConfig());
    }

    private static JSONObject copyConfig(JSONObject config) {
        return JSONObject.parseObject(JSONObject.toJSONString(config));
    }

    public static void loadConfig(JSONObject config) {
        synchronized (StreamFQLConfig.class) {
            CONFIG = copyConfig(config);
        }
    }

}
