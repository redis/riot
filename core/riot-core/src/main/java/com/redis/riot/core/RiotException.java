package com.redis.riot.core;

@SuppressWarnings("serial")
public class RiotException extends RuntimeException {

	public RiotException(String msg) {
		super(msg);
	}

	public RiotException(String msg, Throwable cause) {
		super(msg, cause);
	}

	public RiotException(Throwable cause) {
		super(cause);
	}

}
