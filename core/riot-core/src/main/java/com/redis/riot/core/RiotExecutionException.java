package com.redis.riot.core;

@SuppressWarnings("serial")
public class RiotExecutionException extends Exception {

	public RiotExecutionException(String message, Throwable cause) {
		super(message, cause);
	}

	public RiotExecutionException(String message) {
		super(message);
	}

	public RiotExecutionException(Throwable cause) {
		super(cause);
	}

}
