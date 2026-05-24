package com.learn.kafka.consumer.monitoring.consumer.exception;

public class RetryException extends Exception {

    public RetryException(String message) {
        super(message);
    }

     public RetryException(String message, Throwable cause) {
        super(message, cause);
    }
}
