package com.redis.riot.file;

import org.springframework.cloud.aws.core.io.s3.SimpleStorageProtocolResolver;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

public class AmazonS3ProtocolResolver extends SimpleStorageProtocolResolver {

    private final AmazonS3ClientBuilder clientBuilder;

    public AmazonS3ProtocolResolver(AmazonS3ClientBuilder clientBuilder) {
        this.clientBuilder = clientBuilder;
    }

    @Override
    public AmazonS3 getAmazonS3() {
        return clientBuilder.build();
    }

}
