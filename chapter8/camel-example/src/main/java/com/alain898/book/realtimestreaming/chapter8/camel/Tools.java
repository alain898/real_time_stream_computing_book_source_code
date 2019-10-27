package com.alain898.book.realtimestreaming.chapter8.camel;

public class Tools {
    public static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            // ignore
        }
    }

    public static void waitForever() {
        while (true) {
            sleep(1000);
        }
    }
}
