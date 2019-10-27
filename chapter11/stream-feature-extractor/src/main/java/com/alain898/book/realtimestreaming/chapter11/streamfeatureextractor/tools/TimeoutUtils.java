package com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.tools;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TimeoutUtils {
    private static Logger logger = LoggerFactory.getLogger(TimeoutUtils.class);
    private static ScheduledExecutorService delayer = Executors.createSchedThreadPool("timeout-pool", 1);

    public static <T> CompletableFuture<T> timeoutAfter(long timeout, TimeUnit unit) {
        CompletableFuture<T> result = new CompletableFuture<T>();
        delayer.schedule(() -> {
            result.complete(null);
        }, timeout, unit);
        return result;
    }
}
