package com.redis.riot.file;

import org.springframework.core.io.ProtocolResolver;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.io.WritableResource;

public class StdOutProtocolResolver implements ProtocolResolver {

	public static final String DEFAULT_FILENAME = SystemOutResource.FILENAME;

	private String filename = DEFAULT_FILENAME;

	public void setFilename(String filename) {
		this.filename = filename;
	}

	@Override
	public WritableResource resolve(String location, ResourceLoader resourceLoader) {
		if (location.equals(filename)) {
			return new SystemOutResource();
		}
		return null;
	}
}
