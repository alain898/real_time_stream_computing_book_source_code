package com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.tools;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;


public class Executors {

    private static final Logger logger = LoggerFactory.getLogger(Executors.class);

    private static Map<String, ExecutorService> executors = new HashMap<>();


    public synchronized static void shutdownAll() {
        for (ExecutorService executor : executors.values()) {
            if (!executor.isShutdown()) {
                executor.shutdown();
            }
        }
    }

    public synchronized static ExecutorService createSingleThreadPool(String name) {
        Preconditions.checkArgument(!executors.containsKey(name), "%s executor exists", name);
        ExecutorService executorService =
                java.util.concurrent.Executors.newSingleThreadExecutor(new NameableThreadFactory(name, true));
        executors.put(name, executorService);
        return executorService;
    }

    public synchronized static ScheduledExecutorService createSingleSchedThreadPool(String name) {
        Preconditions.checkArgument(!executors.containsKey(name), "%s executor exists", name);
        ScheduledExecutorService scheduledExecutorService =
                java.util.concurrent.Executors.newSingleThreadScheduledExecutor(new NameableThreadFactory(name, true));
        executors.put(name, scheduledExecutorService);
        return scheduledExecutorService;
    }

    public synchronized static ScheduledExecutorService createSchedThreadPool(String name, int threadNum) {
        Preconditions.checkArgument(!executors.containsKey(name), "%s executor exists", name);
        ScheduledExecutorService scheduledExecutorService =
                java.util.concurrent.Executors.newScheduledThreadPool(threadNum, new NameableThreadFactory(name, true));
        executors.put(name, scheduledExecutorService);
        return scheduledExecutorService;
    }

    public synchronized static ExecutorService createFixedThreadPool(String name, int size) {
        return createFixedThreadPool(name, size, true);
    }

    public synchronized static ExecutorService createFixedThreadPool(String name, int size, boolean isDaemon) {
        Preconditions.checkArgument(!executors.containsKey(name), "%s executor exists", name);
        ExecutorService executorService =
                java.util.concurrent.Executors.newFixedThreadPool(size, new NameableThreadFactory(name, isDaemon));
        executors.put(name, executorService);
        return executorService;
    }

    public synchronized static ExecutorService createFixedThreadPool(String name, int size, boolean isDaemon,
                                                                     BlockingQueue<Runnable> workQueue) {
        Preconditions.checkArgument(!executors.containsKey(name), "%s executor exists", name);
        ExecutorService executorService =
                new ThreadPoolExecutor(size, size, 0L, TimeUnit.MILLISECONDS, workQueue,
                        new NameableThreadFactory(name, isDaemon));
        executors.put(name, executorService);
        return executorService;
    }

    public synchronized static ExecutorService createCachedThreadPool(String name) {
        return createCachedThreadPool(name, true);
    }

    public synchronized static ExecutorService createCachedThreadPool(String name, boolean isDaemon) {
        Preconditions.checkArgument(!executors.containsKey(name), "%s executor exists", name);
        ExecutorService executorService =
                java.util.concurrent.Executors.newCachedThreadPool(new NameableThreadFactory(name, isDaemon));
        executors.put(name, executorService);
        return executorService;
    }

    public synchronized static ExecutorService createCachedThreadPool(String name, int coreSize, int maxSize) {
        Preconditions.checkArgument(!executors.containsKey(name), "%s executor exists", name);
        ExecutorService executorService =
                new ThreadPoolExecutor(coreSize, maxSize, 30L, TimeUnit.SECONDS, new SynchronousQueue<>(),
                        new NameableThreadFactory(name, true), new ThreadPoolExecutor.CallerRunsPolicy());
        executors.put(name, executorService);
        return executorService;
    }

    public synchronized static ExecutorService createMultiQueueThreadPool(String name, int executorNumber, int coreSize, int maxSize, int capacity, long rejectSleepMills) {
        Preconditions.checkArgument(!executors.containsKey(name), "%s executor exists", name);
        ExecutorService executorService =
                new BackPressureExecutor(name, executorNumber, coreSize, maxSize, capacity, rejectSleepMills);
        executors.put(name, executorService);
        return executorService;
    }

    private static class NameableThreadFactory implements ThreadFactory {

        private final ThreadGroup group;
        private final String namePrefix;
        private final AtomicInteger threadId;
        private final boolean isDaemon;

        private NameableThreadFactory(String name, boolean isDaemon) {
            SecurityManager s = System.getSecurityManager();
            this.group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
            this.namePrefix = name;
            this.threadId = new AtomicInteger(0);
            this.isDaemon = isDaemon;
        }

        public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r, namePrefix + "-" + threadId.getAndIncrement());
            t.setDaemon(isDaemon);
            if (t.getPriority() != Thread.NORM_PRIORITY) {
                t.setPriority(Thread.NORM_PRIORITY);
            }
            return t;
        }
    }
}
