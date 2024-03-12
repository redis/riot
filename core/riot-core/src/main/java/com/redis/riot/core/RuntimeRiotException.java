package com.redis.riot.core;

public class RuntimeRiotException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public RuntimeRiotException() {
        super();
    }

    public RuntimeRiotException(String message) {
        super(message);
    }

    public RuntimeRiotException(Throwable cause) {
        super(cause);
    }

    public RuntimeRiotException(String message, Throwable cause) {
        super(message, cause);
    }

}
