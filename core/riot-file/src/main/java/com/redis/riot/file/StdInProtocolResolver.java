package com.redis.riot.file;

import org.springframework.core.io.ProtocolResolver;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

public class StdInProtocolResolver implements ProtocolResolver {

	public static final String DEFAULT_FILENAME = SystemInResource.FILENAME;

	private String filename;

	public StdInProtocolResolver() {
		setFilename(DEFAULT_FILENAME);
	}

	public String getFilename() {
		return filename;
	}

	public void setFilename(String filename) {
		this.filename = filename;
	}

	@Override
	public Resource resolve(String location, ResourceLoader resourceLoader) {
		if (location.equals(filename)) {
			return new SystemInResource();
		}
		return null;
	}
}
