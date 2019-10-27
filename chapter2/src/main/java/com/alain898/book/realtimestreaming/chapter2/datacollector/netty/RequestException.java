package com.alain898.book.realtimestreaming.chapter2.datacollector.netty;

/**
 * Created by alain on 18/5/8.
 */
public class RequestException extends RuntimeException {
    private final int code;
    private final String response;

    public RequestException(int code, String response) {
        this.code = code;
        this.response = response;
    }

    public int getCode() {
        return code;
    }

    public String getResponse() {
        return response;
    }
}
