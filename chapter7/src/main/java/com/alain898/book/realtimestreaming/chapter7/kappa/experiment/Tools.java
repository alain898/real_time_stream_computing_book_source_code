package com.alain898.book.realtimestreaming.chapter7.kappa.experiment;

public class Tools {
    public static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            // ignore
        }
    }
}
