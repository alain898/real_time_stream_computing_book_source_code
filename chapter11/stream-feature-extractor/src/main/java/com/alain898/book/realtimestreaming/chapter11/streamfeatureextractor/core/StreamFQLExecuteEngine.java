package com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.core;

import com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.tools.ConfigHolder;
import com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.tools.ServiceExecutorHolder;
import com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.tools.TimeoutUtils;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class StreamFQLExecuteEngine {
    private static final Logger logger = LoggerFactory.getLogger(StreamFQLExecuteEngine.class);

    private final String DSL_DEPTH_PATTERN_STR = "\\d+";
    private final Pattern DSL_DEPTH_PATTERN = Pattern.compile(DSL_DEPTH_PATTERN_STR);

    private static long EXTRACTOR_TIMEOUT_MS = 3000000;

    protected static final String FIXED_CONTENT = ConfigHolder.getString("schema.fixed_content");

    private boolean isDepthKey(String key) {
        return key != null && DSL_DEPTH_PATTERN.matcher(key).matches();
    }

    private CompletableFuture<JSONObject> executeAsync(Map<String, Set<StreamFQL>> dslTree,
                                                       JSONObject event,
                                                       Map<String, Object> helper,
                                                       String mode,
                                                       String depth,
                                                       Map<StreamFQL, CompletableFuture<Map<String, Object>>> functionFuturesContainer) {
        Map<StreamFQL, CompletableFuture<Map<String, Object>>> currentDepthFunctionFutures = new HashMap<>();
        for (StreamFQL function : dslTree.get(depth)) {
            CompletableFuture<Map<String, Object>> future = CompletableFuture.supplyAsync(() -> {
                        try {
                            return execute(function, event, helper, mode);
                        } catch (Exception e) {
                            logger.error(String.format(
                                    "exception caught, function[%s], event[%s], helper[%s], mode[%s]",
                                    JSONObject.toJSONString(function), JSONObject.toJSONString(event),
                                    JSONObject.toJSONString(helper), mode), e);
                            return new HashMap<>();
                        }
                    },
                    ServiceExecutorHolder.getExtractExecutorService(depth));
            currentDepthFunctionFutures.put(function, future);
            functionFuturesContainer.put(function, future);
        }

        CompletableFuture<Void> allFutures = CompletableFuture.allOf(
                currentDepthFunctionFutures.values().toArray(new CompletableFuture[0]));

        CompletableFuture<JSONObject> result = allFutures.thenApply(v -> {
            event.putIfAbsent(FIXED_CONTENT, new JSONObject());

            for (Map.Entry<StreamFQL, CompletableFuture<Map<String, Object>>> entry :
                    currentDepthFunctionFutures.entrySet()) {
                StreamFQL function = entry.getKey();
                Map<String, Object> functionResult = entry.getValue().join();
                event.getJSONObject(FIXED_CONTENT).put(function.getName(), functionResult.get("value"));
            }
            return event;
        });

        if ("1".equals(depth)) {
            return result;
        } else {
            return result.thenCompose(v -> {
                String newDepth = String.valueOf(Integer.parseInt(depth) - 1);
                return executeAsync(dslTree, event, helper, mode, newDepth, functionFuturesContainer);
            });
        }
    }

    private Map<String, Object> execute(StreamFQL fql,
                                        JSONObject event,
                                        Map<String, Object> helper,
                                        String mode) throws Exception {
        return StreamFQLRegistry.getFunction(fql.getOp()).execute(fql, event, helper, mode);
    }

    public CompletableFuture<JSONObject> executeTreeAsync(Map<String, Set<StreamFQL>> dslTree,
                                                          JSONObject event,
                                                          Map<String, Object> helper,
                                                          String mode) {
        Preconditions.checkNotNull(dslTree, "dslTree is null");
        Preconditions.checkNotNull(event, "event is null");
        Preconditions.checkNotNull(helper, "helper is null");
        Preconditions.checkNotNull(mode, "mode is null");

        int maxDepth = dslTree.keySet().stream().filter(this::isDepthKey)
                .map(Integer::parseInt).max(Integer::compareTo).orElse(0);

        if (maxDepth == 0) {
            throw new IllegalStateException(String.format("invalid dslTree[%s]", JSONObject.toJSONString(dslTree)));
        }

        Map<StreamFQL, CompletableFuture<Map<String, Object>>> functionFuturesContainer = new HashMap<>();
        CompletableFuture<JSONObject> timer = TimeoutUtils.timeoutAfter(EXTRACTOR_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        CompletableFuture<JSONObject> resultFuture = executeAsync(dslTree, event, helper, mode,
                String.valueOf(maxDepth), functionFuturesContainer);
        CompletableFuture<Map<StreamFQL, Map<String, Object>>> allHaveDone = resultFuture
                .applyToEither(timer, v -> {
                    if (!timer.isDone()) {
                        timer.cancel(true);
                    }

                    List<Map.Entry<StreamFQL, CompletableFuture<Map<String, Object>>>> notDone =
                            functionFuturesContainer.entrySet().stream()
                                    .filter(e -> !e.getValue().isDone()).collect(Collectors.toList());
                    notDone.forEach(e -> e.getValue().cancel(true));

                    List<Map.Entry<StreamFQL, CompletableFuture<Map<String, Object>>>> done =
                            functionFuturesContainer.entrySet().stream()
                                    .filter(e -> e.getValue().isDone()).collect(Collectors.toList());

                    final Map<StreamFQL, Map<String, Object>> doneFunctionResults = new HashMap<>();
                    done.forEach(e -> doneFunctionResults.put(e.getKey(), e.getValue().join()));
                    logger.info(String.format("features total[%d], done[%d], not_done[%d]",
                            functionFuturesContainer.size(), done.size(), notDone.size()));
                    return doneFunctionResults;
                });

        return allHaveDone.thenApply(functionResult -> {
            event.putIfAbsent("features", new JSONObject());
            functionResult.forEach((function, result) -> {
                event.getJSONObject("features").put(function.getText_name(), result);
                function.getAlias().forEach(alias -> event.getJSONObject("features").put(alias, new HashMap<>(result)));
            });
            return event;
        }).exceptionally(e -> {
            logger.error(String.format("exception caught, dslTree[%s], event[%s], mode[%s]",
                    String.valueOf(dslTree), String.valueOf(event), mode), e);
            return event;
        });
    }

    public JSONObject executeTree(Map<String, Set<StreamFQL>> dslTree,
                                  JSONObject event,
                                  String mode) throws ExecutionException, InterruptedException {
        Map<String, Object> helper = new ConcurrentHashMap<>();
        return executeTree(dslTree, event, helper, mode);
    }


    public JSONObject executeTree(Map<String, Set<StreamFQL>> dslTree,
                                  JSONObject event,
                                  Map<String, Object> helper,
                                  String mode) throws ExecutionException, InterruptedException {
        Preconditions.checkNotNull(dslTree, "dslTree is null");
        Preconditions.checkNotNull(event, "event is null");
        Preconditions.checkNotNull(helper, "helper is null");
        Preconditions.checkNotNull(mode, "mode is null");

        CompletableFuture<JSONObject> completableFuture = executeTreeAsync(dslTree, event, helper, mode);
        return completableFuture.get();
    }
}
