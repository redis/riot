package com.redis.riot.file;

import java.nio.file.Path;

import lombok.ToString;

@ToString(exclude = "encodedKey")
public class GoogleStorageOptions {

	private Path keyFile;
	private String projectId;
	private String encodedKey;

	public Path getKeyFile() {
		return keyFile;
	}

	public void setKeyFile(Path keyFile) {
		this.keyFile = keyFile;
	}

	public String getProjectId() {
		return projectId;
	}

	public void setProjectId(String projectId) {
		this.projectId = projectId;
	}

	public String getEncodedKey() {
		return encodedKey;
	}

	public void setEncodedKey(String encodedKey) {
		this.encodedKey = encodedKey;
	}

}
