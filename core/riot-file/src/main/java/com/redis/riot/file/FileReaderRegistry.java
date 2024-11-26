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
		registry.register(RiotResourceMap.JSON, new JsonReaderFactory());
		registry.register(RiotResourceMap.JSON_LINES, new JsonLinesReaderFactory());
		registry.register(RiotResourceMap.XML, new XmlReaderFactory());
		registry.register(RiotResourceMap.CSV, new DelimitedReaderFactory(FileOptions.DELIMITER_COMMA));
		registry.register(RiotResourceMap.PSV, new DelimitedReaderFactory(FileOptions.DELIMITER_PIPE));
		registry.register(RiotResourceMap.TSV, new DelimitedReaderFactory(FileOptions.DELIMITER_TAB));
		registry.register(RiotResourceMap.TEXT, new FixedWidthReaderFactory());
		return registry;
	}

}
