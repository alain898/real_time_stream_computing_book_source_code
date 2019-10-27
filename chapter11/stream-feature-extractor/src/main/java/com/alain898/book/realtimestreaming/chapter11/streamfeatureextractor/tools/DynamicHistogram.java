package com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.tools;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AtomicDouble;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DynamicHistogram {

    private static final int BINS_CAPACITY = 64;

    private final int binsCapacity;

    private final TreeMap<Bin, Bin> binMap = new TreeMap<>();

    public DynamicHistogram(int binsCapacity) {
        Preconditions.checkNotNull(binsCapacity > 0, "binsCapacity[%d] must be positive", binsCapacity);
        this.binsCapacity = binsCapacity;
    }

    public DynamicHistogram() {
        this(BINS_CAPACITY);
    }

    public DynamicHistogram add(Bin bin) {
        Preconditions.checkNotNull(bin, "bin is null");

        Bin oldBin = binMap.get(bin);
        if (oldBin == null) {
            binMap.put(bin, bin);
        } else {
            oldBin.count = oldBin.count + bin.count;
        }

        if (binMap.size() > binsCapacity) {
            AtomicReference<Bin> lastBin = new AtomicReference<>(null);
            AtomicDouble minInterval = new AtomicDouble();
            AtomicReference<Bin> minBin1 = new AtomicReference<>(null);
            AtomicReference<Bin> minBin2 = new AtomicReference<>(null);
            binMap.forEach((k, v) -> {
                if (lastBin.get() != null) {
                    if (v.value - lastBin.get().value < minInterval.get()) {
                        minBin1.set(lastBin.get());
                        minBin2.set(v);
                        minInterval.set(v.value - lastBin.get().value);
                    }
                } else {
                    minInterval.set(Double.MAX_VALUE);
                }
                lastBin.set(v);
            });
            float newBinCount = minBin1.get().count + minBin2.get().count;
            float newBinValue = (minBin1.get().value * minBin1.get().count + minBin2.get().value * minBin2.get().count)
                    / (minBin1.get().count + minBin2.get().count);
            Bin newBin = new Bin(newBinValue, newBinCount);
            binMap.remove(minBin1.get());
            binMap.remove(minBin2.get());
            binMap.put(newBin, newBin);
        }
        return this;
    }

    public DynamicHistogram merge(DynamicHistogram hist) {
        Preconditions.checkNotNull(hist, "hist is null");
        int maxCapacity = Math.max(this.binsCapacity, hist.binsCapacity);
        DynamicHistogram dynamicHistogram = new DynamicHistogram(maxCapacity);
        binMap.forEach((k, v) -> dynamicHistogram.add(v));
        hist.binMap.forEach((k, v) -> dynamicHistogram.add(v));
        return dynamicHistogram;
    }

    public int getBinsCapacity() {
        return binsCapacity;
    }

    public List<Bin> getBins() {
        return binMap.entrySet().stream().map(Map.Entry::getValue).collect(Collectors.toList());
    }

    public void setBins(List<Bin> bins) {
        Preconditions.checkNotNull(bins, "bins is null");
        binMap.clear();
        bins.forEach(this::add);
    }

    public byte[] toBytes() {
        List<byte[]> bytes = getBins().stream()
                .flatMap(b -> Lists.newArrayList(ByteUtils.float2Bytes(b.value), ByteUtils.float2Bytes(b.count)).stream())
                .collect(Collectors.toList());

        ByteBuffer bb = ByteBuffer.allocate(bytes.size() * 4);
        bytes.forEach(bb::put);
        return bb.array();
    }

    public static DynamicHistogram fromBytes(byte[] bytes) {
        return fromBytes(BINS_CAPACITY, bytes);
    }

    public static DynamicHistogram fromBytes(int binsCapacity, byte[] bytes) {
        if (bytes.length % 8 != 0 || binsCapacity < bytes.length / 8) {
            throw new IllegalArgumentException(String.format("invalid binsCapacity[%s], bytes[%s]",
                    binsCapacity, JSONObject.toJSONString(bytes)));
        }

        List<Bin> binList = new ArrayList<>(bytes.length / 8);
        for (int i = 0; i < bytes.length; i += 8) {
            byte[] valueBytes = ArrayUtils.subarray(bytes, i, i + 4);
            byte[] countBytes = ArrayUtils.subarray(bytes, i + 4, i + 8);
            binList.add(new Bin(ByteUtils.bytes2Float(valueBytes), ByteUtils.bytes2Float(countBytes)));
        }

        DynamicHistogram dynamicHistogram = new DynamicHistogram(binsCapacity);
        dynamicHistogram.setBins(binList);
        return dynamicHistogram;
    }

    public static class Quantile {
        public static final float Q1 = 0.25f;
        public static final float Q2 = 0.5f;
        public static final float Q3 = 0.75f;

        public float min;
        public float q1;
        public float q2;
        public float q3;
        public float max;
        public float mode;

        public Quantile(float min, float q1, float q2, float q3, float max, float mode) {
            this.min = min;
            this.q1 = q1;
            this.q2 = q2;
            this.q3 = q3;
            this.max = max;
            this.mode = mode;
        }
    }

    public static Quantile quantile(List<Bin> bins, int precision) {
        Preconditions.checkArgument(CollectionUtils.isNotEmpty(bins), "bins is empty");
        if (bins.size() == 1) {
            float b = bins.get(0).getValue();
            return new Quantile(b, b, b, b, b, b);
        }

        float total = bins.stream().map(Bin::getCount).reduce(Float::sum).orElse(0f);
        float min = bins.get(0).getValue();
        float max = bins.get(bins.size() - 1).getValue();
        float delta = (max - min) / precision;
        float minQ1 = Float.MAX_VALUE;
        float q1 = Float.MAX_VALUE;
        float minQ2 = Float.MAX_VALUE;
        float q2 = Float.MAX_VALUE;
        float minQ3 = Float.MAX_VALUE;
        float q3 = Float.MAX_VALUE;

        List<Float> bs = IntStream.range(0, precision)
                .mapToObj(i -> min + delta / 2 + delta * i)
                .collect(Collectors.toList());
        float sum_i = 0;
        int bIndex = 0;
        for (int i = 0; i < bins.size() - 1; i++) {
            if (bIndex == bs.size()) {
                break;
            }
            float b = bs.get(bIndex);
            while (b >= bins.get(i).getValue() && b < bins.get(i + 1).getValue()) {
                float mb = bins.get(i).getCount() +
                        (bins.get(i + 1).getCount() - bins.get(i).getCount())
                                / (bins.get(i + 1).getValue() - bins.get(i).getValue())
                                * (b - bins.get(i).getValue());
                float s = (bins.get(i).getCount() + mb) / 2 * (b - bins.get(i).getValue()) / (
                        bins.get(i + 1).getValue() - bins.get(i).getValue());
                float sum = sum_i + s + bins.get(i).getCount() / 2;

                float rate = sum / total;
                if (Math.abs(rate - Quantile.Q1) < minQ1) {
                    minQ1 = Math.abs(rate - Quantile.Q1);
                    q1 = b;
                }
                if (Math.abs(rate - Quantile.Q2) < minQ2) {
                    minQ2 = Math.abs(rate - Quantile.Q2);
                    q2 = b;
                }
                if (Math.abs(rate - Quantile.Q3) < minQ3) {
                    minQ3 = Math.abs(rate - Quantile.Q3);
                    q3 = b;
                } else {
                    // quick finish
                    bIndex = bs.size();
                    break;
                }

                bIndex++;
                if (bIndex == bs.size()) {
                    break;
                }
                b = bs.get(bIndex);
            }
            sum_i += bins.get(i).getCount();
        }

        float mode = bins.stream()
                .max((o1, o2) -> Float.compare(o1.count, o2.count))
                .orElse(new Bin(0, 0)).getValue();
        return new Quantile(min, q1, q2, q3, max, mode);
    }

    public static class Bin implements Comparable<Bin> {
        private float value;
        private float count;

        public Bin(float value, float count) {
            this.value = value;
            this.count = count;
        }

        public float getValue() {
            return value;
        }

        public void setValue(float value) {
            this.value = value;
        }

        public float getCount() {
            return count;
        }

        public void setCount(float count) {
            this.count = count;
        }

        @Override
        public int compareTo(@NotNull Bin o) {
            return Float.compare(this.value, o.value);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Bin bin = (Bin) o;

            return Float.compare(bin.value, value) == 0;
        }

        @Override
        public int hashCode() {
            return (value != +0.0f ? Float.floatToIntBits(value) : 0);
        }
    }
}
