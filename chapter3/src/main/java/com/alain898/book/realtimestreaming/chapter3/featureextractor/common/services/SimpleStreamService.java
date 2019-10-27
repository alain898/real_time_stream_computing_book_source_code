package com.alain898.book.realtimestreaming.chapter3.featureextractor.common.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public abstract class SimpleStreamService<I, O> extends AbstractStreamService<I, O> {
    private static final Logger logger = LoggerFactory.getLogger(SimpleStreamService.class);

    protected long inputTimeout = 1;
    protected long outputTimeout = 10;

    private List<I> inputsList = new ArrayList<>(1);

    protected SimpleStreamService(String name,
                                  List<ServiceInterface> upstreams,
                                  List<Queue<I>> inputQueues,
                                  List<Queue<O>> outputQueues) {
        super(name, upstreams, inputQueues, outputQueues);
    }

    @Override
    protected void beforeStart() throws Exception {
        logger.info("beforeStart " + this.getName());
    }

    @Override
    protected List<I> poll(List<Queue<I>> inputQueues) throws Exception {
        inputsList.clear();
        Queue<I> inputQueue = inputQueues.get(0);

        I event = inputQueue.poll(inputTimeout, TimeUnit.MILLISECONDS);
        if (event != null) {
            inputsList.add(event);
            return inputsList;
        } else {
            return null;
        }
    }

    @Override
    protected boolean offer(List<Queue<O>> outputQueues, List<O> outputs) throws Exception {
        Queue<O> outputQueue = outputQueues.get(0);
        O event = outputs.get(0);
        return outputQueue.offer(event, outputTimeout, TimeUnit.MILLISECONDS);
    }

    @Override
    protected void beforeShutdown() throws Exception {
        logger.info("beforeShutdown " + this.getName());
    }
}
