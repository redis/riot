package com.redislabs.riot.cli.file;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;

import lombok.Builder;
import lombok.Data;

@Builder
public @Data class SimpleAWSCredentialsProvider implements AWSCredentialsProvider {

	private String accessKey;
	private String secretKey;

	@Override
	public AWSCredentials getCredentials() {
		return new BasicAWSCredentials(accessKey, secretKey);
	}

	@Override
	public void refresh() {
		// do nothing
	}

}
