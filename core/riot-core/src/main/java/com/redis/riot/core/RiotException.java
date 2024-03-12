package com.redis.riot.core;

public class RiotException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public RiotException(String message) {
		super(message);
	}

	public RiotException(String message, Throwable cause) {
		super(message, cause);
	}

	public RiotException(Throwable cause) {
		super(cause);
	}

}
