package com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.core;

import com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.core.functions.StreamFQLResult;
import com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.tools.ConfigHolder;
import com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.tools.JsonTool;
import com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.tools.ServiceExecutorHolder;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang3.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

public class StreamFQLExecutePipe {

    private static final Logger logger = LoggerFactory.getLogger(StreamFQLExecutePipe.class);

    private static final String APPLICATION_PATH = ConfigHolder.getString("schema.application_path");
    private static final String EVENT_TYPE_PATH = ConfigHolder.getString("schema.event_type_path");
    private static final String FIXED_CONTENT = ConfigHolder.getString("schema.fixed_content");


    private JSONObject decode(Object event) {
        Preconditions.checkNotNull(event, "event is null");

        if (byte[].class.isInstance(event)) {
            return JSONObject.parseObject(new String((byte[]) event, Charsets.UTF_8));
        } else if (String.class.isInstance(event)) {
            return JSONObject.parseObject((String) event);
        } else if (JSONObject.class.isInstance(event)) {
            return (JSONObject) event;
        } else {
            throw new IllegalArgumentException(String.format("unsupported event[%s] type[%s]",
                    String.valueOf(event), event.getClass()));
        }
    }

    private JSONArray genFieldArray(String path) {
        JSONArray jsonArray = new JSONArray();
        JSONObject field = new JSONObject();
        field.put("field", path);
        jsonArray.add(field);
        return jsonArray;
    }

    private JSONObject prepareFields(JSONObject appConfig, JSONObject event) throws Exception {
        JSONObject requiredFields = new JSONObject();

        // add schema fields
        JSONObject schemaFields = new JSONObject();
        schemaFields.put("application", genFieldArray(APPLICATION_PATH));
        schemaFields.put("event_type", genFieldArray(EVENT_TYPE_PATH));
        requiredFields.putAll(schemaFields);

        // add default fields
        JSONObject fields = appConfig.getJSONObject("fields");
        JSONObject defaultFields = fields.getJSONObject("___DEFAULT___");
        if (MapUtils.isNotEmpty(defaultFields)) {
            requiredFields.putAll(defaultFields);
        }

        // add fields required in all event types
        JSONObject fieldsRequiredInAllEventTypes = fields.getJSONObject("___ALL___");
        if (MapUtils.isNotEmpty(fieldsRequiredInAllEventTypes)) {
            requiredFields.putAll(fieldsRequiredInAllEventTypes);
        }

        // add fields required for event type
        String eventType = JsonTool.getValueByPath(event, EVENT_TYPE_PATH, String.class);
        JSONObject fieldsRequiredForEventType = fields.getJSONObject(eventType);
        if (MapUtils.isNotEmpty(fieldsRequiredForEventType)) {
            requiredFields.putAll(fieldsRequiredForEventType);
        }

        // prepare required fields
        for (Map.Entry<String, Object> entry : requiredFields.entrySet()) {
            String fieldName = entry.getKey();
            JSONArray value = (JSONArray) entry.getValue();
            for (Object o : value) {
                JSONObject fieldSetting = (JSONObject) o;
                String path = fieldSetting.getString("field");
                boolean success = copyField(event, path, FIXED_CONTENT, fieldName);
                if (success) {
                    break;
                }
            }
        }
        return event;
    }

    private boolean copyField(JSONObject event, String source, String dest, String key) throws Exception {
        Preconditions.checkArgument(event != null);
        Preconditions.checkArgument(source != null);
        Preconditions.checkArgument(dest != null);

        String[] path = dest.split("\\.");
        // create parent dir
        JSONObject jsonObject = event;
        for (String childPath : path) {
            jsonObject.putIfAbsent(childPath, new JSONObject());
            jsonObject = jsonObject.getJSONObject(childPath);
        }
        Object o = JsonTool.getValueByPath(event, source, Object.class);
        if (o instanceof JSONObject) {
            o = SerializationUtils.clone((JSONObject) o);
        }
        jsonObject.put(key, o);
        return o != null;
    }


    private Map<String, Map<String, Set<StreamFQL>>> parseExecutionTree(JSONObject appConfig, JSONObject event) {

        StreamFQLParser streamFQLParser = new StreamFQLParser();
        JSONObject applicationSetting = appConfig.getJSONObject("setting");

        String eventType = JsonTool.getValueByPath(event, EVENT_TYPE_PATH, String.class);
        JSONObject features = appConfig.getJSONObject("features");
        if (features == null) {
            return new HashMap<>();
        }

        Set<String> updateArray = new HashSet<>();
        Set<String> upgetArray = new HashSet<>();
        Set<String> getArray = new HashSet<>();
        JSONObject featuresForAllEventType = features.getJSONObject("___ALL___");
        if (featuresForAllEventType != null) {
            JSONArray update = featuresForAllEventType.getJSONArray("update");
            if (update != null) {
                updateArray.addAll(streamFQLParser.normalizeDSL(
                        Arrays.asList(update.toArray(new String[0]))));
            }
            JSONArray upget = featuresForAllEventType.getJSONArray("upget");
            if (upget != null) {
                upgetArray.addAll(streamFQLParser.normalizeDSL(
                        Arrays.asList(upget.toArray(new String[0]))));
            }
            JSONArray get = featuresForAllEventType.getJSONArray("get");
            if (get != null) {
                getArray.addAll(streamFQLParser.normalizeDSL(
                        Arrays.asList(get.toArray(new String[0]))));
            }
        }
        JSONObject featuresForEventType = features.getJSONObject(eventType);
        if (featuresForEventType != null) {
            JSONArray update = featuresForEventType.getJSONArray("update");
            if (update != null) {
                updateArray.addAll(streamFQLParser.normalizeDSL(
                        Arrays.asList(update.toArray(new String[0]))));
            }
            JSONArray upget = featuresForEventType.getJSONArray("upget");
            if (upget != null) {
                upgetArray.addAll(streamFQLParser.normalizeDSL(
                        Arrays.asList(upget.toArray(new String[0]))));
            }
            JSONArray get = featuresForEventType.getJSONArray("get");
            if (get != null) {
                getArray.addAll(streamFQLParser.normalizeDSL(
                        Arrays.asList(get.toArray(new String[0]))));
            }
        }

        upgetArray.addAll(Sets.intersection(updateArray, getArray));
        updateArray.removeAll(upgetArray);
        getArray.removeAll(upgetArray);
        Map<String, Map<String, Set<StreamFQL>>> trees = new HashMap<>();
        if (CollectionUtils.isNotEmpty(updateArray)) {
            trees.put("update", streamFQLParser.parseExecutionTree(
                    applicationSetting, Arrays.asList(updateArray.toArray(new String[0])), true));
        }
        if (CollectionUtils.isNotEmpty(upgetArray)) {
            trees.put("upget", streamFQLParser.parseExecutionTree(
                    applicationSetting, Arrays.asList(upgetArray.toArray(new String[0])), true));
        }
        if (CollectionUtils.isNotEmpty(getArray)) {
            trees.put("get", streamFQLParser.parseExecutionTree(
                    applicationSetting, Arrays.asList(getArray.toArray(new String[0])), true));
        }
        return trees;
    }


    private CompletableFuture<JSONObject> extract(JSONObject event) {
        String application = JsonTool.getValueByPath(event, APPLICATION_PATH, String.class);
        Preconditions.checkNotNull(application, "application is null for path[%s] in event[%s]",
                APPLICATION_PATH, event);

        JSONObject appConfig = StreamFQLConfig.getConfig().getJSONObject(application);

        // prepared fields
        try {
            prepareFields(appConfig, event);
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format("prepareFields failed, appConfig[%s], event[%s]",
                    JSONObject.toJSONString(appConfig), JSONObject.toJSONString(event)));
        }

        // parse feature
        Map<String, Map<String, Set<StreamFQL>>> trees = parseExecutionTree(appConfig, event);

        // extract features
        StreamFQLExecuteEngine streamFQLExecuteEngine = new StreamFQLExecuteEngine();
        final Map<String, Object> helper = new ConcurrentHashMap<>();
        CompletableFuture<JSONObject> extractFuture = null;
        Map<String, Set<StreamFQL>> update = trees.get(StreamFQLMode.UPDATE);
        if (MapUtils.isNotEmpty(update)) {
            extractFuture = streamFQLExecuteEngine.executeTreeAsync(update, event, helper, StreamFQLMode.UPDATE);
        }
        Map<String, Set<StreamFQL>> upget = trees.get(StreamFQLMode.UPGET);
        if (MapUtils.isNotEmpty(upget)) {
            if (extractFuture == null) {
                extractFuture = streamFQLExecuteEngine.executeTreeAsync(upget, event, helper, StreamFQLMode.UPGET);
            } else {
                extractFuture = extractFuture.thenCompose(e ->
                        streamFQLExecuteEngine.executeTreeAsync(upget, event, helper, StreamFQLMode.UPGET));
            }
        }
        Map<String, Set<StreamFQL>> get = trees.get(StreamFQLMode.GET);
        if (MapUtils.isNotEmpty(get)) {
            if (extractFuture == null) {
                extractFuture = streamFQLExecuteEngine.executeTreeAsync(get, event, helper, StreamFQLMode.GET);
            } else {
                extractFuture = extractFuture.thenCompose(e ->
                        streamFQLExecuteEngine.executeTreeAsync(get, event, helper, StreamFQLMode.GET));
            }
        }
        if (extractFuture == null) {
            extractFuture = CompletableFuture.completedFuture(event);
        }
        extractFuture.thenApply(e -> {
            JSONObject features = e.getJSONObject("features");
            JSONObject normFeatures = new JSONObject();
            for (Map.Entry<String, Object> entry : features.entrySet()) {
                if (Map.class.isInstance(entry.getValue())) {
                    Map featureResultMap = (Map) entry.getValue();
                    Object featureValue = featureResultMap.get("value");
                    if (StreamFQLResult.class.isInstance(featureValue)) {
                        StreamFQLResult streamFQLResult = (StreamFQLResult) featureValue;
                        JSONObject newValue = new JSONObject();
                        newValue.put("value", streamFQLResult.getResult());
                        normFeatures.put(entry.getKey(), newValue);
                        continue;
                    }
                }
                normFeatures.put(entry.getKey(), entry.getValue());
            }
            e.put("features", normFeatures);
            return event;
        }).exceptionally(e -> {
            logger.error(String.format("exception caught, event[%s]", event), e);
            return event;
        });

        return extractFuture;
    }

    public CompletableFuture<JSONObject> processAsync(Object event) {
        Preconditions.checkNotNull(event, "event is null");

        return CompletableFuture
                .supplyAsync(() -> this.decode(event), ServiceExecutorHolder.decoderExecutor)
                .thenComposeAsync(this::extract, ServiceExecutorHolder.extractorExecutor)
                .exceptionally(e -> {
                    logger.error(String.format("unexpected exception, event[%s]", JSONObject.toJSONString(event)), e);
                    return this.decode(event);
                });
    }

    public JSONObject process(Object event) throws ExecutionException, InterruptedException {
        CompletableFuture<JSONObject> future = processAsync(event);
        return future.get();
    }
}
