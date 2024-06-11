package com.redis.riot.file;

import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public class AmazonS3Args {

	private static final String S3_PROTOCOL_PREFIX = "s3://";

	@ArgGroup(exclusive = false)
	private AmazonS3CredentialsArgs credentialsArgs;

	@Option(names = "--s3-region", description = "Region to use for the AWS client (e.g. us-west-1).", paramLabel = "<name>")
	private String region;

	@Option(names = "--s3-endpoint", description = "AWS service endpoint either with or without the protocol (e.g. https://sns.us-west-1.amazonaws.com or sns.us-west-1.amazonaws.com).", paramLabel = "<url>")
	private String endpoint;

	public static boolean isSimpleStorageResource(String location) {
		Assert.notNull(location, "Location must not be null");
		return location.toLowerCase().startsWith(S3_PROTOCOL_PREFIX);
	}

	public Resource resource(String location) {
		AmazonS3ClientBuilder clientBuilder = AmazonS3Client.builder();
		if (endpoint == null) {
			if (region != null) {
				clientBuilder.withRegion(region);
			}
		} else {
			clientBuilder.withEndpointConfiguration(new EndpointConfiguration(endpoint, region));
		}
		if (credentialsArgs != null) {
			clientBuilder.withCredentials(credentialsArgs.credentials());
		}
		AmazonS3ProtocolResolver resolver = new AmazonS3ProtocolResolver(clientBuilder);
		resolver.afterPropertiesSet();
		return resolver.resolve(location, new DefaultResourceLoader());
	}

	public AmazonS3CredentialsArgs getCredentialsArgs() {
		return credentialsArgs;
	}

	public void setCredentialsArgs(AmazonS3CredentialsArgs args) {
		this.credentialsArgs = args;
	}

	public String getRegion() {
		return region;
	}

	public void setRegion(String region) {
		this.region = region;
	}

	public String getEndpoint() {
		return endpoint;
	}

	public void setEndpoint(String endpoint) {
		this.endpoint = endpoint;
	}
}
