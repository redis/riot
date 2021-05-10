package com.redislabs.riot.file;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import lombok.Data;
import org.springframework.cloud.aws.core.io.s3.SimpleStorageProtocolResolver;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import picocli.CommandLine.Option;

@Data
public class S3Options {

    @Option(names = "--s3-access", description = "Access key", paramLabel = "<key>")
    private String accessKey;
    @Option(names = "--s3-secret", arity = "0..1", interactive = true, description = "Secret key", paramLabel = "<key>")
    private String secretKey;
    @Option(names = "--s3-region", description = "AWS region", paramLabel = "<name>")
    private String region;

    public Resource resource(String location) {
        AmazonS3ClientBuilder clientBuilder = AmazonS3Client.builder();
        if (region != null) {
            clientBuilder.withRegion(region);
        }
        if (accessKey != null) {
            clientBuilder.withCredentials(new SimpleAWSCredentialsProvider(accessKey, secretKey));
        }
        SimpleStorageProtocolResolver resolver = new SimpleStorageProtocolResolver() {
            @Override
            public AmazonS3 getAmazonS3() {
                return clientBuilder.build();
            }
        };
        resolver.afterPropertiesSet();
        return resolver.resolve(location, new DefaultResourceLoader());
    }
}
