package com.alain898.book.realtimestreaming.chapter11.flinkfeatureextractor;

public class Event {

    public String application;
    public long timestamp;
    public String event_type;
    public String user_id;
    public String device_id;
    public float amount;

    public Event() {
    }

    public Event(String application, long timestamp, String event_type, String user_id, String device_id, float amount) {
        this.application = application;
        this.timestamp = timestamp;
        this.event_type = event_type;
        this.user_id = user_id;
        this.device_id = device_id;
        this.amount = amount;
    }

    public String getApplication() {
        return application;
    }

    public void setApplication(String application) {
        this.application = application;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getEvent_type() {
        return event_type;
    }

    public void setEvent_type(String event_type) {
        this.event_type = event_type;
    }

    public String getUser_id() {
        return user_id;
    }

    public void setUser_id(String user_id) {
        this.user_id = user_id;
    }

    public String getDevice_id() {
        return device_id;
    }

    public void setDevice_id(String device_id) {
        this.device_id = device_id;
    }

    public float getAmount() {
        return amount;
    }

    public void setAmount(float amount) {
        this.amount = amount;
    }
}