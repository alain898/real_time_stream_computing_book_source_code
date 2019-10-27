package com.alain898.book.realtimestreaming.chapter7.kappa.experiment;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

public class EventTimestampPeriodicWatermarks implements AssignerWithPeriodicWatermarks<CountedEvent> {
    private long currentMaxTimestamp;

    @Override
    public long extractTimestamp(CountedEvent element, long previousElementTimestamp) {
        long timestamp = element.timestamp;
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
        return timestamp;
    }

    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentMaxTimestamp);
    }
}