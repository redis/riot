package com.redis.riot.file;

import java.net.URI;

import lombok.ToString;
import software.amazon.awssdk.regions.Region;

@ToString
public class S3Options {

	private String accessKey;
	private String secretKey;
	private Region region;
	private URI endpoint;

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
