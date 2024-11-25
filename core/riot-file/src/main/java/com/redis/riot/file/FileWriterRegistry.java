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
		registry.register(ResourceFactory.JSON, new JsonWriterFactory());
		registry.register(ResourceFactory.JSON_LINES, new JsonLinesWriterFactory());
		registry.register(ResourceFactory.XML, new XmlWriterFactory());
		registry.register(ResourceFactory.CSV, new DelimitedWriterFactory(FileOptions.DELIMITER_COMMA));
		registry.register(ResourceFactory.PSV, new DelimitedWriterFactory(FileOptions.DELIMITER_PIPE));
		registry.register(ResourceFactory.TSV, new DelimitedWriterFactory(FileOptions.DELIMITER_TAB));
		registry.register(ResourceFactory.TEXT, new FormattedWriterFactory());
		return registry;
	}

}
