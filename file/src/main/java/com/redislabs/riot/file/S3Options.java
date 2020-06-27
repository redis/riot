package com.redislabs.riot.file;

import lombok.Getter;
import picocli.CommandLine;

@Getter
public class S3Options {

    @CommandLine.ArgGroup(exclusive = false)
    private Credentials credentials;

    @Getter
    static class Credentials {
        @CommandLine.Option(names = "--s3-access", required = true, description = "AWS S3 access key ID", paramLabel = "<string>")
        private String accessKey;
        @CommandLine.Option(names = "--s3-secret", required = true, arity = "0..1", interactive = true, description = "AWS S3 secret access key", paramLabel = "<string>")
        private String secretKey;
    }

    @CommandLine.Option(names = "--s3-region", description = "AWS region", paramLabel = "<string>")
    private String region;

}
