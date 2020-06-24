package com.redislabs.riot.file;

import java.net.URI;

import org.springframework.cloud.aws.core.io.s3.SimpleStorageProtocolResolver;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;

public class S3ResourceBuilder {

	public static Resource resource(String accessKey, String secretKey, String region, URI uri) {
		AmazonS3 s3 = AmazonS3Client.builder()
				.withCredentials(
						SimpleAWSCredentialsProvider.builder().accessKey(accessKey).secretKey(secretKey).build())
				.withRegion(region).build();
		SimpleStorageProtocolResolver resolver = new SimpleStorageProtocolResolver(s3);
		resolver.afterPropertiesSet();
		return resolver.resolve(uri.toString(), new DefaultResourceLoader());
	}

}
