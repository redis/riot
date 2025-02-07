package com.redis.riot.core;

@SuppressWarnings("serial")
public class RiotException extends RuntimeException {

	public RiotException(String message, Throwable cause) {
		super(message, cause);
	}

	public RiotException(String message) {
		super(message);
	}

	public RiotException(Throwable cause) {
		super(cause);
	}

}
