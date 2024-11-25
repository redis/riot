package com.redis.riot.file;

import java.util.HashMap;
import java.util.Map;

import org.springframework.util.MimeType;

public class FileReaderRegistry {

	private final Map<MimeType, ReaderFactory> factories = new HashMap<>();

	public void register(MimeType type, ReaderFactory factory) {
		factories.put(type, factory);
	}

	public ReaderFactory getReaderFactory(MimeType type) {
		return factories.get(type);
	}

	public static FileReaderRegistry defaultReaderRegistry() {
		FileReaderRegistry registry = new FileReaderRegistry();
		registry.register(ResourceFactory.JSON, new JsonReaderFactory());
		registry.register(ResourceFactory.JSON_LINES, new JsonLinesReaderFactory());
		registry.register(ResourceFactory.XML, new XmlReaderFactory());
		registry.register(ResourceFactory.CSV, new DelimitedReaderFactory(FileOptions.DELIMITER_COMMA));
		registry.register(ResourceFactory.PSV, new DelimitedReaderFactory(FileOptions.DELIMITER_PIPE));
		registry.register(ResourceFactory.TSV, new DelimitedReaderFactory(FileOptions.DELIMITER_TAB));
		registry.register(ResourceFactory.TEXT, new FixedWidthReaderFactory());
		return registry;
	}

}
