package com.redis.riot.file;

import java.util.HashMap;
import java.util.Map;

import org.springframework.util.MimeType;

public class FileWriterRegistry {

	private final Map<MimeType, WriterFactory> factories = new HashMap<>();

	public void register(MimeType type, WriterFactory factory) {
		factories.put(type, factory);
	}

	public WriterFactory getWriterFactory(MimeType type) {
		return factories.get(type);
	}

	public static FileWriterRegistry defaultWriterRegistry() {
		FileWriterRegistry registry = new FileWriterRegistry();
		registry.register(RiotResourceMap.JSON, new JsonWriterFactory());
		registry.register(RiotResourceMap.JSON_LINES, new JsonLinesWriterFactory());
		registry.register(RiotResourceMap.XML, new XmlWriterFactory());
		registry.register(RiotResourceMap.CSV, new DelimitedWriterFactory(FileOptions.DELIMITER_COMMA));
		registry.register(RiotResourceMap.PSV, new DelimitedWriterFactory(FileOptions.DELIMITER_PIPE));
		registry.register(RiotResourceMap.TSV, new DelimitedWriterFactory(FileOptions.DELIMITER_TAB));
		registry.register(RiotResourceMap.TEXT, new FormattedWriterFactory());
		return registry;
	}

}
