package com.redis.riot.file;

import java.util.function.Supplier;

import org.springframework.core.io.ProtocolResolver;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

import io.awspring.cloud.s3.InMemoryBufferingS3OutputStreamProvider;
import io.awspring.cloud.s3.Location;
import io.awspring.cloud.s3.PropertiesS3ObjectContentTypeResolver;
import io.awspring.cloud.s3.S3OutputStreamProvider;
import io.awspring.cloud.s3.S3Resource;
import software.amazon.awssdk.services.s3.S3Client;

public class S3ProtocolResolver implements ProtocolResolver {

	private Supplier<S3Client> clientSupplier;

	private S3Client client;
	private S3OutputStreamProvider outputStreamProvider;

	public void setClientSupplier(Supplier<S3Client> client) {
		this.clientSupplier = client;
	}

	public void setClient(S3Client client) {
		this.client = client;
	}

	public void setOutputStreamProvider(S3OutputStreamProvider outputStreamProvider) {
		this.outputStreamProvider = outputStreamProvider;
	}

	@Override
	public Resource resolve(String location, ResourceLoader resourceLoader) {
		if (location.startsWith(Location.S3_PROTOCOL_PREFIX)) {
			return new S3Resource(location, client(), outputStreamProvider());
		}
		return null;
	}

	private S3Client client() {
		if (client == null) {
			client = clientSupplier.get();
		}
		return client;
	}

	private S3OutputStreamProvider outputStreamProvider() {
		if (outputStreamProvider == null) {
			PropertiesS3ObjectContentTypeResolver contentTypeResolver = new PropertiesS3ObjectContentTypeResolver();
			outputStreamProvider = new InMemoryBufferingS3OutputStreamProvider(client(), contentTypeResolver);
		}
		return outputStreamProvider;
	}

}
