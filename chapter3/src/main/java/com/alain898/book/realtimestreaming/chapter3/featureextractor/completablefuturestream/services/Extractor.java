package com.alain898.book.realtimestreaming.chapter3.featureextractor.completablefuturestream.services;

import com.alain898.book.realtimestreaming.chapter3.featureextractor.simplestream.services.FeatureForker;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;


public class Extractor {
    private static final Logger logger = LoggerFactory.getLogger(FeatureForker.class);

    private final ExecutorService executorService;

    public Extractor(ExecutorService executorService) {
        this.executorService = executorService;
    }

    private JSONObject doFeatureExtract(JSONObject event, String feature) {
        // TODO: extract feature of event
        JSONObject result = new JSONObject();
        result.put(feature, feature);
        return result;
    }

    private List<CompletableFuture<JSONObject>> fork(final JSONObject event) {
        List<CompletableFuture<JSONObject>> futures = new ArrayList<>();
        final String[] features = {"feature1", "feature2", "feature3"};
        for (String feature : features) {
            CompletableFuture<JSONObject> future = CompletableFuture
                    .supplyAsync(() -> doFeatureExtract(event, feature), executorService);
            futures.add(future);
        }
        return futures;
    }

    public CompletableFuture<JSONObject> extract(JSONObject event) {
        Preconditions.checkNotNull(event, "event is null");

        List<CompletableFuture<JSONObject>> featuresFutures = fork(event);

        return CompletableFuture
                .allOf(featuresFutures.toArray(new CompletableFuture[0]))
                .thenApply(v -> {
                    featuresFutures.forEach(f -> event.putAll(f.join()));
                    return event;
                });
    }
}
