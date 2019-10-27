package com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.core;

public class Equal extends FieldCondition {
    private String value;

    public Equal(String value) {
        super("equal");
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
