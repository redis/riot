package com.redis.riot.file;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.springframework.core.io.Resource;

public abstract class AbstractRegistry<T> {

	private final Map<String, FileType> extensions = new HashMap<>();
	private final Map<FileType, T> factories = new HashMap<>();

	public void register(FileType fileType, T factory) {
		factories.put(fileType, factory);
		if (fileType != null) {
			for (String extension : fileType.getExtensions()) {
				extensions.put(extension, fileType);
			}
		}
	}

	protected T factory(Resource resource, FileOptions options) throws IOException {
		FileType fileType = fileType(resource, options);
		if (fileType == null) {
			throw new UnknownFileTypeException(resource.getFilename());
		}
		T factory = factories.get(fileType);
		if (factory == null) {
			new UnsupportedFileTypeException(fileType);
		}
		return factory;
	}

	private FileType fileType(Resource resource, FileOptions options) {
		if (options.getFileType() == null) {
			return extensions.get(Files.extension(resource.getFilename()));
		}
		return options.getFileType();
	}

}
