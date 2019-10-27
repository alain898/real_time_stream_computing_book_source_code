package com.alain898.book.realtimestreaming.chapter3.featureextractor.simplestream.services;

import com.alain898.book.realtimestreaming.chapter3.featureextractor.common.services.Queue;
import com.alain898.book.realtimestreaming.chapter3.featureextractor.common.services.ServiceInterface;
import com.alain898.book.realtimestreaming.chapter3.featureextractor.common.services.SimpleStreamService;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public class FeatureJoiner extends SimpleStreamService<EventFutureWrapper, JSONObject> {
    private static final Logger logger = LoggerFactory.getLogger(FeatureJoiner.class);


    private final List<JSONObject> outputsList = new ArrayList<>(1);

    private final long extractTimeout = 200;

    public FeatureJoiner(String name, List<ServiceInterface> upstreams,
                         Queue<EventFutureWrapper> inputQueue,
                         Queue<JSONObject> outputQueue) {
        super(name, upstreams, Lists.newArrayList(inputQueue), Lists.newArrayList(outputQueue));
    }

    @Override
    protected List<JSONObject> process(List<EventFutureWrapper> inputs) throws Exception {
        EventFutureWrapper eventFutureWrapper = inputs.get(0);
        if (eventFutureWrapper == null) {
            return null;
        }
        Future<List<JSONObject>> future = eventFutureWrapper.getFuture();

        JSONObject event = eventFutureWrapper.getEvent();
        try {
            List<JSONObject> features = future.get(extractTimeout, TimeUnit.MILLISECONDS);
            JSONObject featureJson = new JSONObject();
            for (JSONObject feature : features) {
                if (feature != null) {
                    featureJson.putAll(feature);
                }
            }
            event.put("features", featureJson);
        } catch (TimeoutException e) {
            logger.error(String.format("feature extract timeout[%d]", extractTimeout));
            future.cancel(true);
            event.put("features", new JSONObject());
        }

        outputsList.clear();
        outputsList.add(event);
        return outputsList;
    }
}
