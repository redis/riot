package com.redis.riot.file;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Base64;

import org.springframework.cloud.gcp.autoconfigure.storage.GcpStorageAutoConfiguration;
import org.springframework.cloud.gcp.core.GcpScope;
import org.springframework.cloud.gcp.core.UserAgentHeaderProvider;
import org.springframework.cloud.gcp.storage.GoogleStorageResource;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.storage.StorageOptions;

import picocli.CommandLine.Option;

public class GcsOptions {

	@Option(names = "--gcs-key-file", description = "GCS private key (e.g. /usr/local/key.json)", paramLabel = "<file>")
	private File credentials;
	@Option(names = "--gcs-project", description = "GCP project id", paramLabel = "<id>")
	private String projectId;
	@Option(names = "--gcs-key", arity = "0..1", interactive = true, description = "GCS Base64 encoded key", paramLabel = "<key>")
	private String encodedKey;

	public File getCredentials() {
		return credentials;
	}

	public void setCredentials(File credentials) {
		this.credentials = credentials;
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

	public GoogleStorageResource resource(String locationUri, boolean readOnly) throws IOException {
		StorageOptions.Builder builder = StorageOptions.newBuilder().setProjectId(ServiceOptions.getDefaultProjectId())
				.setHeaderProvider(new UserAgentHeaderProvider(GcpStorageAutoConfiguration.class));
		if (credentials != null) {
			builder.setCredentials(GoogleCredentials.fromStream(Files.newInputStream(credentials.toPath()))
					.createScoped((readOnly ? GcpScope.STORAGE_READ_ONLY : GcpScope.STORAGE_READ_WRITE).getUrl()));
		}
		if (encodedKey != null) {
			builder.setCredentials(
					GoogleCredentials.fromStream(new ByteArrayInputStream(Base64.getDecoder().decode(encodedKey))));
		}
		if (projectId != null) {
			builder.setProjectId(projectId);
		}
		return new GoogleStorageResource(builder.build().getService(), locationUri);
	}

}
