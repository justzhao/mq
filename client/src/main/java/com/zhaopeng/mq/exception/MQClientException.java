package com.zhaopeng.mq.exception;

/**
 * Created by zhaopeng on 2017/4/23.
 */
public class MQClientException extends Exception {

    private int responseCode;
    private String errorMessage;

    public MQClientException(int responseCode, String errorMessage) {
        this.responseCode = responseCode;
        this.errorMessage = errorMessage;
    }

    public MQClientException(String message) {
        super(message);
    }

    public MQClientException(String message, Throwable cause) {
        super(message, cause);
    }

    @Override
    public String toString() {
        return "MQClientException{" +
                "responseCode=" + responseCode +
                ", errorMessage='" + errorMessage + '\'' +
                '}';
    }
}
