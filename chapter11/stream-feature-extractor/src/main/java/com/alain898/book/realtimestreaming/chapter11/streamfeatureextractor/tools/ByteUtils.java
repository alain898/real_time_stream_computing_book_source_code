package com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.tools;

import java.nio.ByteBuffer;

public class ByteUtils {
    public static byte[] double2Bytes(double d) {
        return ByteBuffer.allocate(8).putDouble(d).array();
    }

    public static double bytes2Double(byte[] arr) {
        return  ByteBuffer.wrap(arr).getDouble();
    }

    public static byte[] float2Bytes(float d) {
        return ByteBuffer.allocate(4).putFloat(d).array();
    }

    public static float bytes2Float(byte[] arr) {
        return  ByteBuffer.wrap(arr).getFloat();
    }
}
