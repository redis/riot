package com.redis.riot.file;

import java.io.File;

import org.springframework.cloud.gcp.core.GcpScope;

import picocli.CommandLine.Option;

public class GoogleStorageArgs {

	public static final GcpScope DEFAULT_SCOPE = GcpScope.STORAGE_READ_ONLY;

	@Option(names = "--gcs-key-file", description = "GCS private key (e.g. /usr/local/key.json).", paramLabel = "<file>")
	private File keyFile;
	
	@Option(names = "--gcs-project", description = "GCP project id.", paramLabel = "<id>")
	private String projectId;
	
	@Option(names = "--gcs-key", arity = "0..1", interactive = true, description = "GCS Base64 encoded key.", paramLabel = "<key>")
	private String encodedKey;
	
	private GcpScope scope = DEFAULT_SCOPE;

	public GcpScope getScope() {
		return scope;
	}

	public void setScope(GcpScope scope) {
		this.scope = scope;
	}

	public File getKeyFile() {
		return keyFile;
	}

	public void setKeyFile(File file) {
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

}
