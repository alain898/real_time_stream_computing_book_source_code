package com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.tools;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class BackPressureExecutor implements ExecutorService {
    private static final Logger logger = LoggerFactory.getLogger(BackPressureExecutor.class);
    private final List<ExecutorService> executors;
    private final Partitioner partitioner;
    private Long rejectSleepMills = 1L;

    public BackPressureExecutor(String name, int executorNumber, int coreSize, int maxSize, int capacity, long rejectSleepMills) {
        Preconditions.checkArgument(executorNumber > 0, "executorNumber must be positive");
        Preconditions.checkArgument(coreSize > 0, "coreSize must be positive");
        Preconditions.checkArgument(maxSize > 0, "maxSize must be positive");
        Preconditions.checkArgument(capacity > 0, "capacity must be positive");

        this.rejectSleepMills = rejectSleepMills;
        this.executors = new ArrayList<>(executorNumber);
        for (int i = 0; i < executorNumber; i++) {
            ArrayBlockingQueue<Runnable> queue = new ArrayBlockingQueue<>(capacity);
            this.executors.add(new ThreadPoolExecutor(
                    coreSize, maxSize, 0L, TimeUnit.MILLISECONDS,
                    queue,
                    new ThreadFactoryBuilder().setNameFormat(name + "-" + i + "-%d").build(),
                    new ThreadPoolExecutor.AbortPolicy()));

        }
        this.partitioner = new RoundRobinPartitionSelector(executorNumber);
    }

    private interface Partitioner {
        int getPartition();
    }

    private static class RoundRobinPartitionSelector implements Partitioner {
        private final int partitions;
        private static PositiveAtomicCounter counter = new PositiveAtomicCounter();

        RoundRobinPartitionSelector(int partitions) {
            this.partitions = partitions;
        }

        @Override
        public int getPartition() {
            return counter.incrementAndGet() % partitions;
        }
    }

    private static class PositiveAtomicCounter {
        private final AtomicInteger atom;
        private static final int mask = 0x7FFFFFFF;

        PositiveAtomicCounter() {
            atom = new AtomicInteger(0);
        }

        public final int incrementAndGet() {
            final int rt = atom.incrementAndGet();
            return rt & mask;
        }

        public int intValue() {
            return atom.intValue();
        }
    }

    @Override
    public void shutdown() {
        for (ExecutorService executor : executors) {
            executor.shutdown();
        }
    }

    @Override
    public List<Runnable> shutdownNow() {
        List<Runnable> results = new ArrayList<>();
        for (ExecutorService executor : executors) {
            results.addAll(executor.shutdownNow());
        }
        return results;
    }

    @Override
    public boolean isShutdown() {
        for (ExecutorService executor : executors) {
            if (!executor.isShutdown()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isTerminated() {
        for (ExecutorService executor : executors) {
            if (!executor.isTerminated()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        for (ExecutorService executor : executors) {
            if (!executor.awaitTermination(timeout, unit)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        boolean rejected;
        Future<T> future = null;
        do {
            try {
                rejected = false;
                future = executors.get(partitioner.getPartition()).submit(task);
            } catch (RejectedExecutionException e) {
                rejected = true;
                try {
                    TimeUnit.MILLISECONDS.sleep(rejectSleepMills);
                } catch (InterruptedException e1) {
                    logger.error("Reject sleep has been interrupted.", e1);
                }
            }
        } while (rejected);
        return future;
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        boolean rejected;
        Future<T> future = null;
        do {
            try {
                rejected = false;
                future = executors.get(partitioner.getPartition()).submit(task, result);
            } catch (RejectedExecutionException e) {
                rejected = true;
                try {
                    TimeUnit.MILLISECONDS.sleep(rejectSleepMills);
                } catch (InterruptedException e1) {
                    logger.error("Reject sleep has been interrupted.", e1);
                }
            }
        } while (rejected);
        return future;
    }

    @Override
    public Future<?> submit(Runnable task) {
        boolean rejected;
        Future<?> future = null;
        do {
            try {
                rejected = false;
                future = executors.get(partitioner.getPartition()).submit(task);
            } catch (RejectedExecutionException e) {
                rejected = true;
                try {
                    TimeUnit.MILLISECONDS.sleep(rejectSleepMills);
                } catch (InterruptedException e1) {
                    logger.error("Reject sleep has been interrupted.", e1);
                }
            }
        } while (rejected);
        return future;
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        throw new NotImplementedException("not implemented");
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
        throw new NotImplementedException("not implemented");
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        throw new NotImplementedException("not implemented");
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        throw new NotImplementedException("not implemented");
    }

    @Override
    public void execute(Runnable command) {
        boolean rejected;
        do {
            try {
                rejected = false;
                executors.get(partitioner.getPartition()).execute(command);
            } catch (RejectedExecutionException e) {
                rejected = true;
                try {
                    TimeUnit.MILLISECONDS.sleep(rejectSleepMills);
                } catch (InterruptedException e1) {
                    logger.error("Reject sleep has been interrupted.", e1);
                }
            }
        } while (rejected);
    }
}
