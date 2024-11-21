package com.redis.riot.file;

import java.util.function.Supplier;

import org.springframework.core.io.ProtocolResolver;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.util.ClassUtils;

import io.awspring.cloud.s3.InMemoryBufferingS3OutputStreamProvider;
import io.awspring.cloud.s3.Location;
import io.awspring.cloud.s3.PropertiesS3ObjectContentTypeResolver;
import io.awspring.cloud.s3.S3ObjectContentTypeResolver;
import io.awspring.cloud.s3.S3OutputStreamProvider;
import io.awspring.cloud.s3.S3Resource;
import software.amazon.awssdk.services.s3.S3Client;

public class S3ProtocolResolver implements ProtocolResolver, ResourceLoader {

	private Supplier<S3Client> clientSupplier;
	private S3Client client;
	private S3OutputStreamProvider outputStreamProvider;

	@Override
	public Resource resolve(String location, ResourceLoader resourceLoader) {
		if (isS3(location)) {
			return getResource(location);
		}
		return null;
	}

	private boolean isS3(String location) {
		return location.startsWith(Location.S3_PROTOCOL_PREFIX);
	}

	@Override
	public Resource getResource(String location) {
		return new S3Resource(location, client(), outputStreamProvider());
	}

	private S3Client client() {
		if (client == null) {
			client = clientSupplier.get();
		}
		return client;
	}

	public S3OutputStreamProvider outputStreamProvider() {
		if (outputStreamProvider == null) {
			S3ObjectContentTypeResolver contentTypeResolver = new PropertiesS3ObjectContentTypeResolver();
			outputStreamProvider = new InMemoryBufferingS3OutputStreamProvider(client(), contentTypeResolver);
		}
		return outputStreamProvider;
	}

	@Override
	public ClassLoader getClassLoader() {
		return ClassUtils.getDefaultClassLoader();
	}

	public void setClient(S3Client client) {
		this.client = client;
	}

	public void setClientSupplier(Supplier<S3Client> clientSupplier) {
		this.clientSupplier = clientSupplier;
	}

}
