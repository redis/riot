package com.redis.riot.file;

import java.net.URI;

import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

public class S3Options {

	private String accessKey;
	private String secretKey;
	private Region region;
	private URI endpoint;

	public S3Client client() {
		S3ClientBuilder clientBuilder = S3Client.builder();
		if (region != null) {
			clientBuilder.region(region);
		}
		if (endpoint != null) {
			clientBuilder.endpointOverride(endpoint);
		}
		clientBuilder.credentialsProvider(credentialsProvider());
		return clientBuilder.build();
	}

	private AwsCredentialsProvider credentialsProvider() {
		if (accessKey == null && secretKey == null) {
			return AnonymousCredentialsProvider.create();
		}
		return StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey));
	}

	public String getAccessKey() {
		return accessKey;
	}

	public void setAccessKey(String accessKey) {
		this.accessKey = accessKey;
	}

	public String getSecretKey() {
		return secretKey;
	}

	public void setSecretKey(String secretKey) {
		this.secretKey = secretKey;
	}

	public Region getRegion() {
		return region;
	}

	public void setRegion(Region region) {
		this.region = region;
	}

	public URI getEndpoint() {
		return endpoint;
	}

	public void setEndpoint(URI endpoint) {
		this.endpoint = endpoint;
	}

}
