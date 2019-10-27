package com.alain898.book.realtimestreaming.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;


public class TimeTools {
    private static final Logger logger = LoggerFactory.getLogger(TimeTools.class);

    public static void sleepSec(long seconds) {
        try {
            TimeUnit.SECONDS.sleep(seconds);
        } catch (InterruptedException e) {
            logger.warn("InterruptedException caught, exit");
        }
    }

    public static void sleepMS(long ms) {
        try {
            TimeUnit.MILLISECONDS.sleep(ms);
        } catch (InterruptedException e) {
            logger.warn("InterruptedException caught, exit");
        }
    }
}
