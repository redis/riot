package com.redislabs.riot.file;

import lombok.Data;
import picocli.CommandLine.Option;

@Data
public class S3Options {

    @Option(names = "--s3-access", description = "Access key", paramLabel = "<key>")
    private String accessKey;
    @Option(names = "--s3-secret", arity = "0..1", interactive = true, description = "Secret key", paramLabel = "<key>")
    private String secretKey;
    @Option(names = "--s3-region", description = "AWS region", paramLabel = "<name>")
    private String region;

}
