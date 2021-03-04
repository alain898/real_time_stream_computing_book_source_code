package com.alain898.book.realtimestreaming.chapter4.streamapi;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class BlockingMapTest {

    private static class BlockingMap<K, V> {
        private final Map<K, V> map;
        private final int capacity;
        private final int offerIntervalMs;

        public BlockingMap(int capacity, int offerIntervalMs) {
            this.capacity = capacity > 0 ? capacity : 32;
            this.map = new ConcurrentHashMap<>(this.capacity);
            this.offerIntervalMs = offerIntervalMs > 0 ? offerIntervalMs : 10;
        }

        public V remove(K key) {
            return map.remove(key);
        }

        public void put(K key, V value) throws InterruptedException {
            do {
                if (this.map.size() < capacity) {
                    synchronized (this.map) {
                        if (this.map.size() < capacity) {
                            this.map.put(key, value);
                            break;
                        }
                    }
                }
                Thread.sleep(offerIntervalMs);
            } while (true);
        }

        public void clear() {
            this.map.clear();
        }
    }

    public static void main(String[] args) {

        Map<Integer, String> map = new ConcurrentHashMap<>();
        int samples = 100000;
        for (int i = 0; i < samples; i++) {
            map.put(i, String.format("%d", i));
        }
        long start = System.currentTimeMillis();
        long sum = 0;
        int counts = 10000;
        for (int i = 0; i < counts; i++) {
            sum += map.size();
        }
        long end = System.currentTimeMillis();
        System.out.println(String.format("sum[%d],counts[%d], time[%d], avg_time[%f]",
                sum, counts, end - start, (end - start) / (float) counts));

    }
}
