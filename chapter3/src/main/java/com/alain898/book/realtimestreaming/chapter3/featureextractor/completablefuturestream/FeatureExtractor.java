package com.alain898.book.realtimestreaming.chapter3.featureextractor.completablefuturestream;

import com.alain898.book.realtimestreaming.chapter3.featureextractor.completablefuturestream.services.Decoder;
import com.alain898.book.realtimestreaming.chapter3.featureextractor.completablefuturestream.services.Extractor;
import com.alain898.book.realtimestreaming.chapter3.featureextractor.completablefuturestream.services.Receiver;
import com.alain898.book.realtimestreaming.chapter3.featureextractor.completablefuturestream.services.Sender;
import com.alain898.book.realtimestreaming.common.TimeTools;
import com.alain898.book.realtimestreaming.common.concurrency.BackPressureExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


public class FeatureExtractor {

    private static final Logger logger = LoggerFactory.getLogger(FeatureExtractor.class);

    private final int receiverNumber = 2;
    private final ExecutorService decoderExecutor = new BackPressureExecutor(
            "decoderExecutor", 1, 2, 1024, 1024, 1);
    private final ExecutorService extractExecutor = new BackPressureExecutor(
            "extractExecutor", 1, 4, 1024, 1024, 1);
    private final ExecutorService senderExecutor = new BackPressureExecutor(
            "senderExecutor", 1, 2, 1024, 1024, 1);
    private final ExecutorService extractService = new BackPressureExecutor(
            "extractService", 1, 16, 1024, 1024, 1);

    private final List<Thread> receivers = IntStream.range(0, this.receiverNumber)
            .mapToObj(i -> new Thread(new ReceiverRunnable(), String.format("receiver-%d", i)))
            .collect(Collectors.toList());
    private final Decoder decoder = new Decoder();
    private final Extractor extractor = new Extractor(extractService);
    private final Sender sender = new Sender();

    private volatile boolean stop = false;


    private class ReceiverRunnable implements Runnable {
        private final Receiver receiver = new Receiver();

        @Override
        public void run() {
            while (!stop && !(Thread.currentThread().isInterrupted())) {
                byte[] event = receiver.receive();
                if (event == null) {
                    continue;
                }

                CompletableFuture
                        .supplyAsync(() -> decoder.decode(event), decoderExecutor)
                        .thenComposeAsync(extractor::extract, extractExecutor)
                        .thenAcceptAsync(sender::send, senderExecutor)
                        .exceptionally(e -> {
                            logger.error("unexpected exception", e);
                            return null;
                        });
            }
        }
    }

    public void start() {
        receivers.forEach(Thread::start);
    }

    public void stop() {
        stop = true;
        receivers.forEach(Thread::interrupt);

        receivers.forEach(t -> {
            try {
                t.join();
            } catch (InterruptedException e) {
                logger.warn("InterruptedException caught, exit");
            }
        });

        shutdownAndWait(decoderExecutor);
        shutdownAndWait(extractExecutor);
        shutdownAndWait(extractService);
        shutdownAndWait(senderExecutor);
    }

    private void shutdownAndWait(ExecutorService executorService) {
        executorService.shutdown();
        try {
            executorService.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.warn("InterruptedException caught, exit");
        }
    }


    public static void main(String[] args) {
        FeatureExtractor featureExtractor = new FeatureExtractor();
        featureExtractor.start();
        TimeTools.sleepSec(100);
        featureExtractor.stop();
    }
}
