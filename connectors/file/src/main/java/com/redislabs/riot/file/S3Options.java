package com.redislabs.riot.file;

import lombok.Getter;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

@Getter
public class S3Options {

	@ArgGroup(exclusive = false)
	private Credentials credentials;

	@Getter
	static class Credentials {
		@Option(names = "--s3-access", required = true, description = "S3 access key ID", paramLabel = "<string>")
		private String accessKey;
		@Option(names = "--s3-secret", required = true, arity = "0..1", interactive = true, description = "S3 secret access key", paramLabel = "<string>")
		private String secretKey;
	}

	@Option(names = "--s3-region", description = "AWS region", paramLabel = "<string>")
	private String region;

}
