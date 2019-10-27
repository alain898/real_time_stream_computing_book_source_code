package com.alain898.book.realtimestreaming.chapter3.featureextractor.common.services;

import java.util.concurrent.TimeUnit;


public interface Queue<E> {
    E poll(long timeout, TimeUnit unit) throws InterruptedException;

    boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException;
}
