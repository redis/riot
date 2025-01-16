package com.redis.riot;

import java.nio.file.Path;

import com.redis.riot.file.GoogleStorageOptions;

import lombok.ToString;
import picocli.CommandLine.Option;

@ToString
public class GoogleStorageArgs {

	@Option(names = "--gcs-key-file", description = "Google Cloud Storage private key (e.g. /usr/local/key.json).", paramLabel = "<file>")
	private Path keyFile;

	@Option(names = "--gcs-project", description = "Google Cloud Storage project id.", paramLabel = "<id>")
	private String projectId;

	@Option(names = "--gcs-key", arity = "0..1", interactive = true, description = "Google Cloud Storage Base64 encoded key.", paramLabel = "<key>")
	private String encodedKey;

	public Path getKeyFile() {
		return keyFile;
	}

	public void setKeyFile(Path file) {
		this.keyFile = file;
	}

	public String getProjectId() {
		return projectId;
	}

	public void setProjectId(String id) {
		this.projectId = id;
	}

	public String getEncodedKey() {
		return encodedKey;
	}

	public void setEncodedKey(String key) {
		this.encodedKey = key;
	}

	public GoogleStorageOptions googleStorageOptions() {
		GoogleStorageOptions options = new GoogleStorageOptions();
		options.setEncodedKey(encodedKey);
		options.setKeyFile(keyFile);
		options.setProjectId(projectId);
		return options;
	}
}
