package com.alain898.book.realtimestreaming.chapter7.kappa.experiment;

import java.util.HashSet;
import java.util.Set;

public class CountedEvent extends Event {
    public int _count;
    public Set<Long> _times; // 用于验证同一个window内的所有事件的timestamp是否确实落在这个window内
    public static final int FAST_LAYER_SLIDE_STEP_IN_SECONDS = 15;
    public static final int BATCH_LAYER_SLIDE_STEP_IN_SECONDS = 5;
    public long _minTimestamp;

    public CountedEvent() {
        super();
        this._count = 0;
        this._times = new HashSet<>();
        this._minTimestamp = Long.MAX_VALUE;
    }

    public CountedEvent(String product, long timestamp, long slide_window_in_sec) {
        super(product, timestamp);
        this._count = 1;
        this._times = new HashSet<>();
        // _times仅仅用于验证同一个window内的所有事件的timestamp是否确实落在这个window内，实际使用时可以去掉
        this._times.add(timestamp / (slide_window_in_sec * 1000));
        this._minTimestamp = timestamp;
    }

    @Override
    public String toString() {
        return "CountedEvent{" +
                "count=" + _count +
                ", _times=" + _times +
                ", minTimestamp=" + _minTimestamp +
                ", product='" + product + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
