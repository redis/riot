package com.redis.riot;

import lombok.ToString;
import picocli.CommandLine.Option;

@ToString
public class AwsCredentialsArgs {

	@Option(names = "--s3-access", required = true, description = "AWS access key.", paramLabel = "<key>")
	private String accessKey;

	@Option(names = "--s3-secret", required = true, arity = "0..1", interactive = true, description = "AWS secret key.", paramLabel = "<key>")
	private String secretKey;

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