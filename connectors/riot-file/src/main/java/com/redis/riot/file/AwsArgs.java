package com.redis.riot.file;

import java.net.URI;

import org.springframework.core.io.Resource;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import io.awspring.cloud.s3.InMemoryBufferingS3OutputStreamProvider;
import io.awspring.cloud.s3.Location;
import io.awspring.cloud.s3.PropertiesS3ObjectContentTypeResolver;
import io.awspring.cloud.s3.S3Resource;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

public class AwsArgs {

	@ArgGroup(exclusive = false)
	private AwsCredentialsArgs credentialsArgs;

	@Option(names = "--s3-region", description = "Region to use for the AWS client (e.g. us-west-1).", paramLabel = "<name>")
	private Region region;

	@Option(names = "--s3-endpoint", description = "Service endpoint with which the AWS client should communicate (e.g. https://sns.us-west-1.amazonaws.com).", paramLabel = "<url>")
	private URI endpoint;

	public static boolean isSimpleStorageResource(String location) {
		Assert.notNull(location, "Location must not be null");
		return location.toLowerCase().startsWith(Location.S3_PROTOCOL_PREFIX);
	}

	public Resource resource(String location) {
		S3ClientBuilder clientBuilder = S3Client.builder();
		if (region != null) {
			clientBuilder.region(region);
		}
		if (endpoint != null) {
			clientBuilder.endpointOverride(endpoint);
		}
		clientBuilder.credentialsProvider(credentialsProvider());
		S3Client client = clientBuilder.build();
		InMemoryBufferingS3OutputStreamProvider outputStreamProvider = new InMemoryBufferingS3OutputStreamProvider(
				client, new PropertiesS3ObjectContentTypeResolver());
		return S3Resource.create(location, client, outputStreamProvider);
	}

	private AwsCredentialsProvider credentialsProvider() {
		if (credentialsArgs != null && StringUtils.hasText(credentialsArgs.getAccessKey())
				&& StringUtils.hasText(credentialsArgs.getSecretKey())) {
			AwsBasicCredentials credentials = AwsBasicCredentials.create(credentialsArgs.getAccessKey(),
					credentialsArgs.getSecretKey());
			return StaticCredentialsProvider.create(credentials);
		}
		return AnonymousCredentialsProvider.create();
	}

	public AwsCredentialsArgs getCredentialsArgs() {
		return credentialsArgs;
	}

	public void setCredentialsArgs(AwsCredentialsArgs args) {
		this.credentialsArgs = args;
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
