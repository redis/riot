package com.redis.riot.file;

import java.io.InputStream;

import org.springframework.core.io.InputStreamResource;

public class NamedInputStreamResource extends InputStreamResource {

	private final String filename;

	public NamedInputStreamResource(InputStream inputStream, String filename, String description) {
		super(inputStream, description);
		this.filename = filename;
	}

	@Override
	public String getFilename() {
		return filename;
	}

}
