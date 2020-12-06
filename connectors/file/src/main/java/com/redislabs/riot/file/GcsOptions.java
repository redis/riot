package com.redislabs.riot.file;

import java.io.File;

import lombok.Getter;
import picocli.CommandLine.Option;

@Getter
public class GcsOptions {

	@Option(names = "--gcs-key-file", description = "GCS private key (e.g. /usr/local/key.json)", paramLabel = "<file>")
	private File credentials;
	@Option(names = "--gcs-project", description = "GCP project id", paramLabel = "<string>")
	private String projectId;
	@Option(names = "--gcs-key", description = "GCS Base64 encoded key", paramLabel = "<string>")
	private String encodedKey;

}
