package com.alain898.book.realtimestreaming.chapter3.featureextractor.simplestream.services;

import com.alain898.book.realtimestreaming.chapter3.featureextractor.common.services.Queue;
import com.alain898.book.realtimestreaming.chapter3.featureextractor.common.services.ServiceInterface;
import com.alain898.book.realtimestreaming.chapter3.featureextractor.common.services.SimpleStreamService;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class FeatureForker extends SimpleStreamService<JSONObject, EventFutureWrapper> {
    private static final Logger logger = LoggerFactory.getLogger(FeatureForker.class);

    private final List<EventFutureWrapper> outputsList = new ArrayList<>(1);

    private final ExecutorService executorService = Executors.newFixedThreadPool(32);

    public FeatureForker(String name, List<ServiceInterface> upstreams,
                         Queue<JSONObject> inputQueue, Queue<EventFutureWrapper> outputQueue) {
        super(name, upstreams, Lists.newArrayList(inputQueue), Lists.newArrayList(outputQueue));
        this.inputTimeout = 10;
        this.outputTimeout = 10;
    }


    private JSONObject doFeatureExtract(JSONObject event, String feature) {
        // TODO: 实现具体的特征提取
        JSONObject result = new JSONObject();
        result.put(feature, feature);
        return result;
    }

    private class ExtractorRunnable implements Runnable {
        private final JSONObject event;
        private final String feature;
        private final SettableFuture<JSONObject> future;

        private ExtractorRunnable(JSONObject event, String feature, SettableFuture<JSONObject> future) {
            this.event = event;
            this.feature = feature;
            this.future = future;
        }

        @Override
        public void run() {
            try {
                JSONObject result = doFeatureExtract(event, feature);
                future.set(result);
            } catch (Exception e) {
                logger.error("exception caught", e);
                future.set(null);
            }
        }
    }

    private List<SettableFuture<JSONObject>> fork(final JSONObject event) {
        List<SettableFuture<JSONObject>> futures = new ArrayList<>();
        final String[] features = {"feature1", "feature2", "feature3"};
        for (String feature : features) {
            SettableFuture<JSONObject> future = SettableFuture.create();
            executorService.execute(new ExtractorRunnable(event, feature, future));
            futures.add(future);
        }
        return futures;
    }

    @Override
    protected List<EventFutureWrapper> process(List<JSONObject> inputs) throws Exception {
        JSONObject event = inputs.get(0);
        if (event == null) {
            logger.warn(String.format("service[%s] get a null event.", getName()));
            return null;
        }

        List<SettableFuture<JSONObject>> featuresFutures = fork(event);
        ListenableFuture<List<JSONObject>> resultFuture = Futures.allAsList(featuresFutures);

        EventFutureWrapper result = new EventFutureWrapper(event, resultFuture);
        outputsList.clear();
        outputsList.add(result);
        return outputsList;
    }

    @Override
    protected void beforeShutdown() throws Exception {
        super.beforeShutdown();
        executorService.shutdown();
    }
}
