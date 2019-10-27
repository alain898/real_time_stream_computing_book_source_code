package com.alain898.book.realtimestreaming.chapter3.featureextractor.common.services;


public interface ServiceInterface {
    void start();

    void stop();

    void waitToShutdown() throws InterruptedException;

    String getName();

    ServiceStatus getStatus();
}
