package com.redis.riot.core;

public class RiotExecutionException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public RiotExecutionException(String message) {
        super(message);
    }

    public RiotExecutionException(String message, Throwable cause) {
        super(message, cause);
    }

    public RiotExecutionException(Throwable cause) {
        super(cause);
    }

}
