package com.redis.riot.file;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import org.springframework.core.io.Resource;
import org.springframework.core.io.WritableResource;
import org.springframework.util.Assert;
import org.springframework.util.MimeType;

public class FileWriterRegistry extends AbstractRegistry {

	private final Map<MimeType, WriterFactory> factories = new HashMap<>();
	private ResourceMap resourceMap = new RiotResourceMap();

	public ResourceMap getResourceMap() {
		return resourceMap;
	}

	public void setResourceMap(ResourceMap resourceMap) {
		this.resourceMap = resourceMap;
	}

	public void register(MimeType type, WriterFactory factory) {
		factories.put(type, factory);
	}

	public FileWriterResult find(String location, WriteOptions options) {
		WritableResource resource = resource(location, options);
		MimeType type = type(resource, options);
		FileWriterResult result = new FileWriterResult();
		result.setResource(resource);
		result.setType(type);
		WriterFactory factory = factories.get(type);
		if (factory != null) {
			result.setWriter(factory.create(resource, options));
		}
		return result;
	}

	@Override
	protected WritableResource resource(String location, FileOptions options) {
		Resource resource = super.resource(location, options);
		Assert.isInstanceOf(WritableResource.class, resource, "Resource is not writable");
		WritableResource writableResource = (WritableResource) resource;
		if (options.isGzip() || FileUtils.isGzip(resource.getFilename())) {
			GZIPOutputStream gzipOutputStream;
			try {
				gzipOutputStream = new GZIPOutputStream(writableResource.getOutputStream());
			} catch (IOException e) {
				throw new RuntimeIOException("Could not create GZip output stream", e);
			}
			return new OutputStreamResource(gzipOutputStream, resource.getFilename(), resource.getDescription());
		}
		return writableResource;
	}

	public static FileWriterRegistry defaultWriterRegistry() {
		FileWriterRegistry registry = new FileWriterRegistry();
		registry.register(FileUtils.JSON, new JsonWriterFactory());
		registry.register(FileUtils.JSON_LINES, new JsonLinesWriterFactory());
		registry.register(FileUtils.XML, new XmlWriterFactory());
		registry.register(FileUtils.CSV, new DelimitedWriterFactory(FileOptions.DELIMITER_COMMA));
		registry.register(FileUtils.PSV, new DelimitedWriterFactory(FileOptions.DELIMITER_PIPE));
		registry.register(FileUtils.TSV, new DelimitedWriterFactory(FileOptions.DELIMITER_TAB));
		registry.register(FileUtils.TEXT, new FormattedWriterFactory());
		return registry;
	}

}
