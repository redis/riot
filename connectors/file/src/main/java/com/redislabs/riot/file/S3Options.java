package com.redislabs.riot.file;

import lombok.Data;
import picocli.CommandLine.Option;

@Data
public class S3Options {

	@Option(names = "--s3-access", description = "Access key", paramLabel = "<string>")
	private String accessKey;
	@Option(names = "--s3-secret", arity = "0..1", interactive = true, description = "Secret key", paramLabel = "<string>")
	private String secretKey;
	@Option(names = "--s3-region", description = "AWS region", paramLabel = "<string>")
	private String region;

}
