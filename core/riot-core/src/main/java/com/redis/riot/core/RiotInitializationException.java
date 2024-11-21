package com.redis.riot.core;

@SuppressWarnings("serial")
public class RiotInitializationException extends Exception {

	public RiotInitializationException(String message, Throwable cause) {
		super(message, cause);
	}

	public RiotInitializationException(String message) {
		super(message);
	}

	public RiotInitializationException(Throwable cause) {
		super(cause);
	}

}
