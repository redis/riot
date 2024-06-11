package com.redis.riot.file;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;

import picocli.CommandLine.Option;

public class AmazonS3CredentialsArgs {

	@Option(names = "--s3-access", required = true, description = "AWS access key.", paramLabel = "<key>")
	private String accessKey;

	@Option(names = "--s3-secret", required = true, arity = "0..1", interactive = true, description = "AWS secret key.", paramLabel = "<key>")
	private String secretKey;

	public AWSCredentialsProvider credentials() {
		BasicAWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
		return new AWSStaticCredentialsProvider(credentials);
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

}