package com.redislabs.riot.file;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import picocli.CommandLine.Option;

import java.io.File;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class GcsOptions {

	@Option(names = "--gcs-key-file", description = "GCS private key (e.g. /usr/local/key.json)", paramLabel = "<file>")
	private File credentials;
	@Option(names = "--gcs-project", description = "GCP project id", paramLabel = "<id>")
	private String projectId;
	@Option(names = "--gcs-key", arity = "0..1", interactive = true, description = "GCS Base64 encoded key", paramLabel = "<key>")
	private String encodedKey;

}
