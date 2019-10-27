package com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.console.commands;

import com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.core.StreamFQLConfig;
import com.alibaba.fastjson.JSONObject;

public class ConsoleStatus {
    private static final ConsoleStatus INSTANCE = new ConsoleStatus();

    private static final String DEFAULT_APPLICATION_CONFIG_TEMPLATE =
            "{\"setting\": {\"space\": {\"field\": \"application\"}, \"on_default\": [{\"field\": \"application\"}] }, \"fields\": {\"___DEFAULT___\": {\"application\": [{\"field\": \"$.event.application\"}], \"event_type\": [{\"field\": \"$.event.event_type\"}] } } }";

    private String application = null;
    private String source = null;
    private String sink = null;
    private JSONObject applicationConfigs = null;
    private Thread worker = null;
    private volatile boolean stop = false;

    private ConsoleStatus() {
        applicationConfigs = StreamFQLConfig.duplicateConfig();
    }


    public static ConsoleStatus getInstance() {
        return INSTANCE;
    }

    public String getApplication() {
        return application;
    }

    public void setApplication(String application) {
        this.application = application;
    }

    public JSONObject getApplicationConfigs() {
        return applicationConfigs;
    }

    public Thread getWorker() {
        return worker;
    }

    public void setWorker(Thread worker) {
        this.worker = worker;
    }

    public boolean isStop() {
        return stop;
    }

    public void setStop(boolean stop) {
        this.stop = stop;
    }

    public JSONObject newDefaultApplicationConfig() {
        return JSONObject.parseObject(DEFAULT_APPLICATION_CONFIG_TEMPLATE);
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getSink() {
        return sink;
    }

    public void setSink(String sink) {
        this.sink = sink;
    }
}
