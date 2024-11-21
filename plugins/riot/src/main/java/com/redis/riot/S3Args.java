package com.redis.riot;

import java.net.URI;

import com.redis.riot.file.S3Options;

import lombok.ToString;
import picocli.CommandLine.Option;
import software.amazon.awssdk.regions.Region;

@ToString
public class S3Args {

	@Option(names = "--s3-access", description = "AWS access key.", paramLabel = "<key>")
	private String accessKey;

	@Option(names = "--s3-secret", arity = "0..1", interactive = true, description = "AWS secret key.", paramLabel = "<key>")
	private String secretKey;

	@Option(names = "--s3-region", description = "Region to use for the AWS client (e.g. us-west-1).", paramLabel = "<name>")
	private Region region;

	@Option(names = "--s3-endpoint", description = "Service endpoint with which the AWS client should communicate (e.g. https://sns.us-west-1.amazonaws.com).", paramLabel = "<url>")
	private URI endpoint;

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

	public S3Options s3Options() {
		S3Options options = new S3Options();
		options.setAccessKey(accessKey);
		options.setSecretKey(secretKey);
		options.setEndpoint(endpoint);
		options.setRegion(region);
		return options;
	}

}
