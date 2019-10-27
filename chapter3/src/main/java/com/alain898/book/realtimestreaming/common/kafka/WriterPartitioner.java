package com.alain898.book.realtimestreaming.common.kafka;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class WriterPartitioner implements Partitioner {

    /**
     * constructor, avoid java.lang.NoSuchMethodException...(kafka.utils.VerifiableProperties)
     */
    public WriterPartitioner(VerifiableProperties verifiableProperties) {
    }

    @Override
    public int partition(Object key, int numPartitions) {
        if (key == null) {
            throw new NullPointerException("key is not null, key value: " + key);
        }
        return Math.abs(hash(key.toString())) % numPartitions;
    }


    private int hash(String k) {
        int h = 0;
        h ^= k.hashCode();
        h ^= (h >>> 20) ^ (h >>> 12);
        return h ^ (h >>> 7) ^ (h >>> 4);
    }

}
