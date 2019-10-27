package com.alain898.book.realtimestreaming.chapter3.featureextractor.common.services;

import com.google.common.base.Preconditions;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


public abstract class AbstractStreamService<I, O> implements ServiceInterface {
    private static final Logger logger = LoggerFactory.getLogger(AbstractStreamService.class);

    abstract protected void beforeStart() throws Exception;

    abstract protected List<I> poll(List<Queue<I>> inputQueues) throws Exception;

    abstract protected List<O> process(List<I> inputs) throws Exception;

    abstract protected boolean offer(List<Queue<O>> outputQueues, List<O> outputs) throws Exception;

    abstract protected void beforeShutdown() throws Exception;


    private List<ServiceInterface> upstreams;
    private List<Queue<I>> inputQueues;
    private List<Queue<O>> outputQueues;

    private Thread thread;
    private String name = this.toString();

    protected volatile boolean stopped = false;
    protected volatile boolean isShutdown = false;
    private volatile ServiceStatus status = new ServiceStatus(stopped, isShutdown);

    protected AbstractStreamService(String name,
                                    List<ServiceInterface> upstreams,
                                    List<Queue<I>> inputQueues,
                                    List<Queue<O>> outputQueues) {
        Preconditions.checkNotNull(name, "name cannot be null");
        this.name = name;
        this.upstreams = upstreams;
        this.inputQueues = inputQueues;
        this.outputQueues = outputQueues;
    }

    private boolean pipeline(boolean checkStop, boolean exitWhenNoInput) throws Exception {
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("service[%s] checkStop[%s] exitWhenNoInput[%s]",
                    getName(), checkStop, exitWhenNoInput));
        }
        boolean exit = false;
        try {
            List<I> inputs;
            while (true) {
                if (checkStop && stopped) throw new ExitException();
                inputs = poll(inputQueues);
                if (CollectionUtils.isNotEmpty(inputs)) {
                    break;
                } else {
                    if (exitWhenNoInput) throw new ExitException();
                }
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("service[%s] get no input", getName()));
                }
            }

            if (logger.isDebugEnabled()) {
                logger.debug(String.format("service[%s] get %d inputs", getName(), inputs.size()));
            }

            List<O> outputs = process(inputs);

            if (outputs != null && outputs.size() > 0) {
                while (true) {
                    if (offer(outputQueues, outputs)) {
                        if (checkStop && stopped) throw new ExitException();
                        break;
                    }
                    if (logger.isDebugEnabled()) {
                        logger.debug(String.format("service[%s] not all outputs offered", getName()));
                    }
                }
            }
        } catch (InterruptedException e) {
            logger.warn(String.format("InterruptedException caught, service[%s] will exit.", getName()));
            exit = true;
        } catch (ExitException e) {
            logger.info(String.format("service[%s] will exit", getName()));
            exit = true;
        }
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("service[%s] pipeline result[%s]", getName(), exit));
        }
        return exit;
    }

    private boolean allUpstreamsShutdown() {
        boolean shutdown = true;
        if (CollectionUtils.isEmpty(upstreams)) {
            shutdown = true;
        } else {
            for (ServiceInterface service : upstreams) {
                if (service != null && !service.getStatus().isShutdown()) {
                    logger.info(String.format("upstream[%s] not shutdown", service.getName()));
                    shutdown = false;
                    break;
                }
            }
        }
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("service[%s] check allUpstreamsShutdown result[%s]", getName(), shutdown));
        }
        return shutdown;
    }

    @Override
    public void start() {
        thread = new Thread(() -> {
            try {
                beforeStart();

                while (!stopped) {
                    try {
                        if (pipeline(true, false)) {
                            break;
                        }
                    } catch (Exception e) {
                        logger.error("exception caught", e);
                    }
                }
                logger.info(String.format("stopping service[%s]...", getName()));
                while (true) {
                    try {
                        if (pipeline(false, true) && allUpstreamsShutdown()) {
                            break;
                        }
                        if (logger.isDebugEnabled()) {
                            logger.debug(String.format("service[%s] recycle again after been stopped.",
                                    getName()));
                        }
                    } catch (Exception e) {
                        logger.error("exception caught", e);
                    }
                }

                beforeShutdown();
            } catch (Exception e) {
                logger.error("exception caught", e);
            }
            isShutdown = true;
            status = new ServiceStatus(stopped, isShutdown);
        });
        thread.setName(getName());
        thread.start();
    }

    @Override
    public void stop() {
        stopped = true;
        status = new ServiceStatus(stopped, isShutdown);
    }

    @Override
    public void waitToShutdown() throws InterruptedException {
        thread.join();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public ServiceStatus getStatus() {
        return status;
    }
}
