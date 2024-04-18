package com.redis.riot.file;

import java.io.IOException;

public class RuntimeIOException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public RuntimeIOException(String message, IOException e) {
		super(message, e);
	}

}
