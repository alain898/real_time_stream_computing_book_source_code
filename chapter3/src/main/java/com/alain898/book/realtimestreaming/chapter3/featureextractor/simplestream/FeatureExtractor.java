package com.alain898.book.realtimestreaming.chapter3.featureextractor.simplestream;

import com.alain898.book.realtimestreaming.chapter3.featureextractor.common.services.BackPressureQueue;
import com.alain898.book.realtimestreaming.chapter3.featureextractor.common.services.Queue;
import com.alain898.book.realtimestreaming.chapter3.featureextractor.common.services.ServiceInterface;
import com.alain898.book.realtimestreaming.chapter3.featureextractor.simplestream.services.*;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;


public class FeatureExtractor {

    public static void main(String[] args) throws Exception {
        List<ServiceInterface> services = new ArrayList<>();

        int decoderInputQueueCapacity = 1024;
        Queue<byte[]> decoderInputQueue = new BackPressureQueue<>(decoderInputQueueCapacity);
        Receiver receiver1 = new Receiver("receiver-1", decoderInputQueue);
        services.add(receiver1);
        Receiver receiver2 = new Receiver("receiver-2", decoderInputQueue);
        services.add(receiver2);

        int featureForkerInputQueueCapacity = 1024;
        Queue<JSONObject> featureForkerInputQueue = new BackPressureQueue<>(featureForkerInputQueueCapacity);
        Decoder decoder = new Decoder("decoder-1",
                Lists.newArrayList(receiver1, receiver2), decoderInputQueue, featureForkerInputQueue);
        services.add(decoder);

        int featureJoinerInputQueueCapacity = 1024;
        Queue<EventFutureWrapper> featureJoinerInputQueue = new BackPressureQueue<>(featureJoinerInputQueueCapacity);
        FeatureForker featureForker = new FeatureForker("featureforker-1",
                Lists.newArrayList(decoder), featureForkerInputQueue, featureJoinerInputQueue);
        services.add(featureForker);


        int senderInputQueueCapacity = 1024;
        Queue<JSONObject> senderInputQueue = new BackPressureQueue<>(senderInputQueueCapacity);
        FeatureJoiner featureJoiner = new FeatureJoiner("featurejoiner-1",
                Lists.newArrayList(featureForker), featureJoinerInputQueue, senderInputQueue);
        services.add(featureJoiner);

        Sender sender = new Sender("sender-1",
                Lists.newArrayList(featureJoiner), senderInputQueue);
        services.add(sender);

        for (ServiceInterface service : services) {
            service.start();
        }

        TimeUnit.SECONDS.sleep(100);

        for (ServiceInterface service : services) {
            service.stop();
        }

        // wait until shutdown
        for (ServiceInterface service : services) {
            service.waitToShutdown();
        }
    }
}
