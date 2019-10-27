package com.alain898.book.realtimestreaming.chapter11.flinkfeatureextractor;

public class Tools {
    public static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            // ignore
        }
    }
}
