package com.redis.riot.file;

import java.io.IOException;

@SuppressWarnings("serial")
public class UnknownFileTypeException extends IOException {

	private final String file;

	public UnknownFileTypeException(String file) {
		this.file = file;
	}

	public String getFile() {
		return file;
	}

}
