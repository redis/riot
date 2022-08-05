package com.redis.riot.file;

import java.util.Optional;

import org.springframework.cloud.aws.core.io.s3.SimpleStorageProtocolResolver;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import picocli.CommandLine.Option;

public class S3Options {

	@Option(names = "--s3-access", description = "Access key", paramLabel = "<key>")
	private Optional<String> accessKey = Optional.empty();
	@Option(names = "--s3-secret", arity = "0..1", interactive = true, description = "Secret key", paramLabel = "<key>")
	private Optional<String> secretKey = Optional.empty();
	@Option(names = "--s3-region", description = "AWS region", paramLabel = "<name>")
	private Optional<String> region = Optional.empty();

	public void setAccessKey(String accessKey) {
		this.accessKey = Optional.of(accessKey);
	}

	public void setSecretKey(String secretKey) {
		this.secretKey = Optional.of(secretKey);
	}

	public void setRegion(String region) {
		this.region = Optional.of(region);
	}

	public Resource resource(String location) {
		AmazonS3ClientBuilder clientBuilder = AmazonS3Client.builder();
		region.ifPresent(clientBuilder::withRegion);
		if (accessKey.isPresent()) {
			Assert.isTrue(secretKey.isPresent(), "Secret key is missing");
			clientBuilder.withCredentials(new SimpleAWSCredentialsProvider(accessKey.get(), secretKey.get()));
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

	@Override
	public String toString() {
		return "S3Options [accessKey=" + accessKey + ", secretKey=" + secretKey + ", region=" + region + "]";
	}

}
