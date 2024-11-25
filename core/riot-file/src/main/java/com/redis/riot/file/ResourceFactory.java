package com.redis.riot.file;

import java.io.IOException;
import java.net.FileNameMap;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.springframework.core.io.ProtocolResolver;
import org.springframework.core.io.Resource;
import org.springframework.core.io.WritableResource;
import org.springframework.util.Assert;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;

public class ResourceFactory {

	public static final String GZ_SUFFIX = ".gz";

	public static final MimeType CSV = new MimeType("text", "csv");
	public static final MimeType PSV = new MimeType("text", "psv");
	public static final MimeType TSV = new MimeType("text", "tsv");
	public static final MimeType TEXT = new MimeType("text", "plain");
	public static final MimeType JSON = MimeTypeUtils.APPLICATION_JSON;
	public static final MimeType JSON_LINES = new MimeType("application", "jsonlines");
	public static final MimeType XML = MimeTypeUtils.APPLICATION_XML;

	private ResourceMap resourceMap = defaultResourceMap();
	private Set<ProtocolResolver> protocolResolvers = new HashSet<>();

	public MimeType type(Resource resource) {
		return MimeType.valueOf(resourceMap.getContentTypeFor(resource));
	}

	public MimeType type(Resource resource, FileOptions options) {
		if (options.getContentType() == null) {
			return MimeType.valueOf(resourceMap.getContentTypeFor(resource));
		}
		return options.getContentType();
	}

	private static class JsonLinesFileNameMap implements FileNameMap {

		public static final String JSONL_SUFFIX = ".jsonl";

		@Override
		public String getContentTypeFor(String fileName) {
			if (fileName == null) {
				return null;
			}
			if (fileName.endsWith(JSONL_SUFFIX)) {
				return JSON_LINES.toString();
			}
			return null;
		}

	}

	public static ResourceMap defaultResourceMap() {
		RiotResourceMap resourceMap = new RiotResourceMap();
		resourceMap.addFileNameMap(new JsonLinesFileNameMap());
		return resourceMap;
	}

	public void addProtocolResolver(ProtocolResolver protocolResolver) {
		protocolResolvers.add(protocolResolver);
	}

	public Resource resource(String location, FileOptions options) throws IOException {
		Resource resource = createResource(location, options);
		if (isGzip(resource, options)) {
			GZIPInputStream gzipInputStream = new GZIPInputStream(resource.getInputStream());
			return new NamedInputStreamResource(gzipInputStream, resource.getFilename(), resource.getDescription());
		}
		return resource;
	}

	private boolean isGzip(Resource resource, FileOptions options) {
		return options.isGzip() || isGzip(resource.getFilename());
	}

	public WritableResource writableResource(String location, FileOptions options) throws IOException {
		Resource resource = createResource(location, options);
		Assert.isInstanceOf(WritableResource.class, resource);
		if (options.isGzip() || isGzip(resource.getFilename())) {
			GZIPOutputStream gzipOutputStream = new GZIPOutputStream(((WritableResource) resource).getOutputStream());
			return new OutputStreamResource(gzipOutputStream, resource.getFilename(), resource.getDescription());
		}
		return (WritableResource) resource;
	}

	private Resource createResource(String location, FileOptions options) {
		RiotResourceLoader resourceLoader = new RiotResourceLoader();
		protocolResolvers.forEach(resourceLoader::addProtocolResolver);
		resourceLoader.getS3ProtocolResolver().setClientSupplier(options.getS3Options()::client);
		resourceLoader.getGoogleStorageProtocolResolver()
				.setStorageSupplier(options.getGoogleStorageOptions()::storage);
		return resourceLoader.getResource(location);
	}

	public static boolean isGzip(String filename) {
		return filename.endsWith(GZ_SUFFIX);
	}

	public static String stripGzipSuffix(String filename) {
		if (isGzip(filename)) {
			return filename.substring(0, filename.length() - GZ_SUFFIX.length());
		}
		return filename;
	}

	public ResourceMap getResourceMap() {
		return resourceMap;
	}

	public void setResourceMap(ResourceMap resourceMap) {
		this.resourceMap = resourceMap;
	}

}
