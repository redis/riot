package com.redis.riot.file;

import java.io.File;
import java.util.Optional;

import com.google.cloud.spring.core.GcpScope;

import lombok.ToString;

@ToString(exclude = "encodedKey")
public class GcpOptions {

	public static final GcpScope DEFAULT_SCOPE = GcpScope.STORAGE_READ_ONLY;

	private File keyFile;
	private Optional<String> projectId = Optional.empty();
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

	public Optional<String> getProjectId() {
		return projectId;
	}

	public void setProjectId(String id) {
		this.projectId = Optional.ofNullable(id);
	}

	public String getEncodedKey() {
		return encodedKey;
	}

	public void setEncodedKey(String key) {
		this.encodedKey = key;
	}

}
