package com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.tools;


import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

public class ServiceExecutorHolder {

    public static final ExecutorService decoderExecutor =
            Executors.createMultiQueueThreadPool("decoder",
                    ConfigHolder.getInt("decoder.executor_number"),
                    ConfigHolder.getInt("decoder.coreSize"),
                    ConfigHolder.getInt("decoder.maxSize"),
                    ConfigHolder.getInt("decoder.executor_queue_capacity"),
                    ConfigHolder.getLong("decoder.reject_sleep_mills"));

    public static final ExecutorService extractorExecutor =
            Executors.createMultiQueueThreadPool("extractor",
                    ConfigHolder.getInt("extractor.executor_number"),
                    ConfigHolder.getInt("extractor.coreSize"),
                    ConfigHolder.getInt("extractor.maxSize"),
                    ConfigHolder.getInt("extractor.executor_queue_capacity"),
                    ConfigHolder.getLong("extractor.reject_sleep_mills"));


    private static final Map<String, ExecutorService> EXECUTOR_SERVICE_MAP = new ConcurrentHashMap<>();

    public static ExecutorService getExtractExecutorService(String depth) {
        Preconditions.checkArgument(StringUtils.isNotBlank(depth), "depth is blank");

        if (EXECUTOR_SERVICE_MAP.get(depth) != null) {
            return EXECUTOR_SERVICE_MAP.get(depth);
        }
        synchronized (ServiceExecutorHolder.class) {
            if (EXECUTOR_SERVICE_MAP.get(depth) != null) {
                return EXECUTOR_SERVICE_MAP.get(depth);
            }
            EXECUTOR_SERVICE_MAP.put(depth, Executors.createMultiQueueThreadPool(
                    String.format("extract_service_depth_%s", depth),
                    ConfigHolder.getInt("extract_service.executor_number"),
                    ConfigHolder.getInt("extract_service.coreSize"),
                    ConfigHolder.getInt("extract_service.maxSize"),
                    ConfigHolder.getInt("extract_service.executor_queue_capacity"),
                    ConfigHolder.getLong("extract_service.reject_sleep_mills")));
            return EXECUTOR_SERVICE_MAP.get(depth);
        }

    }
}
