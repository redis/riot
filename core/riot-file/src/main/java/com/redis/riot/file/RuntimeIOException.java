package com.redis.riot.file;

import java.io.IOException;

import org.springframework.core.NestedRuntimeException;

@SuppressWarnings("serial")
public class RuntimeIOException extends NestedRuntimeException {

	public RuntimeIOException(String msg) {
		super(msg);
	}

	public RuntimeIOException(String msg, IOException cause) {
		super(msg, cause);
	}

}