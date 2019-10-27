package com.alain898.book.realtimestreaming.chapter7.kappa.example;

public class CountedEvent extends Event {
    public int count;
    // 这里就假定每秒钟都有消息进来了，这样就可以使用[minTimestamp, maxTimestamp]作为MySQL记录的时间区间。
    // 实际上最正确的方式应该是直接使用Window的起始时间和窗口长度作为MySQL记录的时间区间。
    public long minTimestamp;
    public long maxTimestamp;

    public CountedEvent() {
        super();
        this.count = 0;
        this.minTimestamp = Long.MAX_VALUE;
        this.maxTimestamp = Long.MIN_VALUE;
    }

    public CountedEvent(String product, int count, long timestamp) {
        super(product, timestamp);
        this.count = count;
        this.minTimestamp = timestamp;
        this.maxTimestamp = timestamp;
    }

    @Override
    public String toString() {
        return "CountedEvent{" +
                "count=" + count +
                ", minTimestamp=" + minTimestamp +
                ", maxTimestamp=" + maxTimestamp +
                ", product='" + product + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
