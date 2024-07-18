package com.redis.riot.core;

@SuppressWarnings("serial")
public class RiotException extends RuntimeException {

	public RiotException(Throwable e) {
		super(e);
	}

	public RiotException(String message) {
		super(message);
	}

	public RiotException(String message, Throwable e) {
		super(message, e);
	}

}
