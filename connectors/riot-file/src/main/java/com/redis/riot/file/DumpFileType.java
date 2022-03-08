package com.redis.riot.file;

import java.util.Optional;

public enum DumpFileType {

	JSON, XML;

	public static DumpFileType of(String file, Optional<DumpFileType> type) {
		if (type.isPresent()) {
			return type.get();
		}
		Optional<String> extension = FileUtils.extension(file);
		if (extension.isPresent() && extension.get().equalsIgnoreCase(FileUtils.EXTENSION_XML)) {
			return XML;
		}
		return JSON;
	}
}