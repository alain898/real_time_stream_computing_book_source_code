package com.alain898.book.realtimestreaming.chapter3.featureextractor.common.services;

import java.util.concurrent.ArrayBlockingQueue;

public class BackPressureQueue<E> extends ArrayBlockingQueue<E> implements Queue<E> {
    public BackPressureQueue(int capacity) {
        super(capacity);
    }
}


