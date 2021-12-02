package com.redis.riot.file;

import org.springframework.cloud.aws.core.io.s3.SimpleStorageProtocolResolver;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import picocli.CommandLine.Option;

public class S3Options {

	@Option(names = "--s3-access", description = "Access key", paramLabel = "<key>")
	private String accessKey;
	@Option(names = "--s3-secret", arity = "0..1", interactive = true, description = "Secret key", paramLabel = "<key>")
	private String secretKey;
	@Option(names = "--s3-region", description = "AWS region", paramLabel = "<name>")
	private String region;

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

	public String getRegion() {
		return region;
	}

	public void setRegion(String region) {
		this.region = region;
	}

	public Resource resource(String location) {
		AmazonS3ClientBuilder clientBuilder = AmazonS3Client.builder();
		if (region != null) {
			clientBuilder.withRegion(region);
		}
		if (accessKey != null) {
			clientBuilder.withCredentials(new SimpleAWSCredentialsProvider(accessKey, secretKey));
		}
		SimpleStorageProtocolResolver resolver = new SimpleStorageProtocolResolver() {
			@Override
			public AmazonS3 getAmazonS3() {
				return clientBuilder.build();
			}
		};
		resolver.afterPropertiesSet();
		return resolver.resolve(location, new DefaultResourceLoader());
	}

	private static class SimpleAWSCredentialsProvider implements AWSCredentialsProvider {

		private final String accessKey;
		private final String secretKey;

		public SimpleAWSCredentialsProvider(String accessKey, String secretKey) {
			this.accessKey = accessKey;
			this.secretKey = secretKey;
		}

		@Override
		public AWSCredentials getCredentials() {
			return new BasicAWSCredentials(accessKey, secretKey);
		}

		@Override
		public void refresh() {
			// do nothing
		}

	}
}
