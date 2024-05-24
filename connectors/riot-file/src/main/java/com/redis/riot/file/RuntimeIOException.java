package com.redis.riot.file;

import java.io.IOException;

@SuppressWarnings("serial")
public class RuntimeIOException extends RuntimeException {

	public RuntimeIOException(String message, IOException e) {
		super(message, e);
	}

}
