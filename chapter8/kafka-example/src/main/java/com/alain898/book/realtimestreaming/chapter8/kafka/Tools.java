package com.alain898.book.realtimestreaming.chapter8.kafka;

public class Tools {
    public static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            // ignore
        }
    }
}
