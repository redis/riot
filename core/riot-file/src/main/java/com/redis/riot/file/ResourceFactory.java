package com.redis.riot.file;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.springframework.core.io.ProtocolResolver;
import org.springframework.core.io.Resource;
import org.springframework.core.io.WritableResource;
import org.springframework.util.Assert;

public class ResourceFactory {

	public static final String GZ_SUFFIX = ".gz";

	private Set<ProtocolResolver> protocolResolvers = new HashSet<>();

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

}
