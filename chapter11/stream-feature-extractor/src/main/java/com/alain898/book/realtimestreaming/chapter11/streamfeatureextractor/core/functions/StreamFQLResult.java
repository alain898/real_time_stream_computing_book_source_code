package com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.core.functions;

import com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.core.StreamFQL;

public class StreamFQLResult {
    private StreamFQL fql;
    private Object result;

    public StreamFQLResult() {
    }

    public StreamFQLResult(StreamFQL fql, Object result) {
        this.fql = fql;
        this.result = result;
    }

    public StreamFQL getFql() {
        return fql;
    }

    public void setFql(StreamFQL fql) {
        this.fql = fql;
    }

    public Object getResult() {
        return result;
    }

    public void setResult(Object result) {
        this.result = result;
    }
}
