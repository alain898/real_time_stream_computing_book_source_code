package com.alain898.book.realtimestreaming.chapter11.flinkfeatureextractor;

public class Event2 {
    public String application;
    public long timestamp;
    public String event_type;
    public String pay_account;
    public String rcv_account;
    public float amount;

    public Event2(String application, long timestamp, String event_type, String pay_account, String rcv_account, float amount) {
        this.application = application;
        this.timestamp = timestamp;
        this.event_type = event_type;
        this.pay_account = pay_account;
        this.rcv_account = rcv_account;
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

    public String getPay_account() {
        return pay_account;
    }

    public void setPay_account(String pay_account) {
        this.pay_account = pay_account;
    }

    public float getAmount() {
        return amount;
    }

    public void setAmount(float amount) {
        this.amount = amount;
    }

    public String getRcv_account() {
        return rcv_account;
    }

    public void setRcv_account(String rcv_account) {
        this.rcv_account = rcv_account;
    }
}
