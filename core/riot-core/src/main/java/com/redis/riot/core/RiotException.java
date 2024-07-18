package com.redis.riot.core;

@SuppressWarnings("serial")
public class RiotException extends Exception {

	public RiotException(String message, Exception cause) {
		super(message, cause);
	}

	public RiotException(String message) {
		super(message);
	}

	public RiotException(Exception cause) {
		super(cause);
	}

	@Override
	public synchronized Exception getCause() {
		return (Exception) super.getCause();
	}

}
