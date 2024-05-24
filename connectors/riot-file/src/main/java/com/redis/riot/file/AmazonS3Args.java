package com.redis.riot.file;

import picocli.CommandLine.Option;

public class AmazonS3Args {

	@Option(names = "--s3-access", description = "Access key.", paramLabel = "<key>")
	private String accessKey;

	@Option(names = "--s3-secret", arity = "0..1", interactive = true, description = "Secret key.", paramLabel = "<key>")
	private String secretKey;

	@Option(names = "--s3-region", description = "AWS region.", paramLabel = "<name>")
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

}
