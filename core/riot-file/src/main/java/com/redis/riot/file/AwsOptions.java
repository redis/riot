package com.redis.riot.file;

import java.net.URI;
import java.util.Optional;

import lombok.ToString;
import software.amazon.awssdk.regions.Region;

@ToString
public class AwsOptions {

	private AwsCredentials credentials = new AwsCredentials();
	private Optional<Region> region = Optional.empty();
	private Optional<URI> endpoint = Optional.empty();

	public AwsCredentials getCredentials() {
		return credentials;
	}

	public void setCredentials(AwsCredentials credentials) {
		this.credentials = credentials;
	}

	public Optional<Region> getRegion() {
		return region;
	}

	public void setRegion(Region region) {
		this.region = Optional.ofNullable(region);
	}

	public Optional<URI> getEndpoint() {
		return endpoint;
	}

	public void setEndpoint(URI endpoint) {
		this.endpoint = Optional.ofNullable(endpoint);
	}

}
