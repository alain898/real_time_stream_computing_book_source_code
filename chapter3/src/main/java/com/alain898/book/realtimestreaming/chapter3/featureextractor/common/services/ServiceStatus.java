package com.alain898.book.realtimestreaming.chapter3.featureextractor.common.services;


public class ServiceStatus {
    private final boolean isStopped;
    private final boolean isShutdown;

    public ServiceStatus(boolean isStopped, boolean isShutdown) {
        this.isStopped = isStopped;
        this.isShutdown = isShutdown;
    }

    public boolean isStopped() {
        return isStopped;
    }

    public boolean isShutdown() {
        return isShutdown;
    }
}
