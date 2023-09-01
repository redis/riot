package com.redis.riot.core.file;

public class RuntimeIOException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public RuntimeIOException() {
		super();
	}

	public RuntimeIOException(String message) {
		super(message);
	}

	public RuntimeIOException(Throwable cause) {
		super(cause);
	}

	public RuntimeIOException(String message, Throwable cause) {
		super(message, cause);
	}
}