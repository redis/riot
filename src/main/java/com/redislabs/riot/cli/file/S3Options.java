package com.redislabs.riot.cli.file;

import org.springframework.cloud.aws.core.io.s3.SimpleStorageProtocolResolver;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;

import lombok.Data;
import picocli.CommandLine.Option;

public @Data class S3Options implements AWSCredentialsProvider {

	@Option(names = "--s3-access", description = "AWS S3 access key ID", paramLabel = "<string>")
	private String accessKey;
	@Option(names = "--s3-secret", arity = "0..1", interactive = true, description = "AWS S3 secret access key", paramLabel = "<string>")
	private String secretKey;
	@Option(names = "--s3-region", description = "AWS region", paramLabel = "<string>")
	private String region;

	@Override
	public AWSCredentials getCredentials() {
		return new BasicAWSCredentials(accessKey, secretKey);
	}

	@Override
	public void refresh() {
		// do nothing
	}

	public Resource resource(String path) {
		AmazonS3 s3 = AmazonS3Client.builder().withCredentials(this).withRegion(region).build();
		SimpleStorageProtocolResolver resolver = new SimpleStorageProtocolResolver(s3);
		resolver.afterPropertiesSet();
		return resolver.resolve(path, new DefaultResourceLoader());

	}

}
