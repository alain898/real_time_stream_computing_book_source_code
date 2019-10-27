package com.alain898.book.realtimestreaming.chapter3.featureextractor.simplestream.services;

import com.alibaba.fastjson.JSONObject;

import java.util.List;
import java.util.concurrent.Future;


public class EventFutureWrapper {
    private final JSONObject event;
    private final Future<List<JSONObject>> future;

    public EventFutureWrapper(JSONObject event, Future<List<JSONObject>> future) {
        this.event = event;
        this.future = future;
    }

    public JSONObject getEvent() {
        return event;
    }

    public Future<List<JSONObject>> getFuture() {
        return future;
    }
}
