package com.redis.riot.file;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.springframework.core.io.Resource;
import org.springframework.util.MimeType;

public class FileReaderRegistry extends AbstractRegistry {

	private final Map<MimeType, ReaderFactory> factories = new HashMap<>();

	public void register(MimeType type, ReaderFactory factory) {
		factories.put(type, factory);
	}

	public FileReaderResult find(String location, ReadOptions options) {
		Resource resource = resource(location, options);
		MimeType type = type(resource, options);
		FileReaderResult reader = new FileReaderResult();
		reader.setResource(resource);
		reader.setType(type);
		ReaderFactory factory = factories.get(type);
		if (factory != null) {
			reader.setReader(factory.create(resource, options));
		}
		return reader;
	}

	@Override
	protected Resource resource(String location, FileOptions options) {
		Resource resource = super.resource(location, options);
		if (options.isGzip() || FileUtils.isGzip(resource.getFilename())) {
			GZIPInputStream gzipInputStream;
			try {
				gzipInputStream = new GZIPInputStream(resource.getInputStream());
			} catch (IOException e) {
				throw new RuntimeIOException("Could not create GZip input stream", e);
			}
			return new NamedInputStreamResource(gzipInputStream, resource.getFilename(), resource.getDescription());
		}
		return resource;
	}

	public static FileReaderRegistry defaultReaderRegistry() {
		FileReaderRegistry registry = new FileReaderRegistry();
		registry.register(FileUtils.JSON, new JsonReaderFactory());
		registry.register(FileUtils.JSON_LINES, new JsonLinesReaderFactory());
		registry.register(FileUtils.XML, new XmlReaderFactory());
		registry.register(FileUtils.CSV, new DelimitedReaderFactory(FileOptions.DELIMITER_COMMA));
		registry.register(FileUtils.PSV, new DelimitedReaderFactory(FileOptions.DELIMITER_PIPE));
		registry.register(FileUtils.TSV, new DelimitedReaderFactory(FileOptions.DELIMITER_TAB));
		registry.register(FileUtils.TEXT, new FixedWidthReaderFactory());
		return registry;
	}

}
