package com.redis.riot.file;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Base64;
import java.util.Optional;

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
	private Optional<File> credentials = Optional.empty();
	@Option(names = "--gcs-project", description = "GCP project id", paramLabel = "<id>")
	private Optional<String> projectId = Optional.empty();
	@Option(names = "--gcs-key", arity = "0..1", interactive = true, description = "GCS Base64 encoded key", paramLabel = "<key>")
	private Optional<String> encodedKey = Optional.empty();

	public void setCredentials(File credentials) {
		this.credentials = Optional.of(credentials);
	}

	public void setProjectId(String projectId) {
		this.projectId = Optional.of(projectId);
	}

	public void setEncodedKey(String encodedKey) {
		this.encodedKey = Optional.of(encodedKey);
	}

	public GoogleStorageResource resource(String locationUri, boolean readOnly) throws IOException {
		StorageOptions.Builder builder = StorageOptions.newBuilder().setProjectId(ServiceOptions.getDefaultProjectId())
				.setHeaderProvider(new UserAgentHeaderProvider(GcpStorageAutoConfiguration.class));
		if (credentials.isPresent()) {
			builder.setCredentials(GoogleCredentials.fromStream(Files.newInputStream(credentials.get().toPath()))
					.createScoped((readOnly ? GcpScope.STORAGE_READ_ONLY : GcpScope.STORAGE_READ_WRITE).getUrl()));
		}
		if (encodedKey.isPresent()) {
			builder.setCredentials(GoogleCredentials
					.fromStream(new ByteArrayInputStream(Base64.getDecoder().decode(encodedKey.get()))));
		}
		projectId.ifPresent(builder::setProjectId);
		return new GoogleStorageResource(builder.build().getService(), locationUri);
	}

	@Override
	public String toString() {
		return "GcsOptions [credentials=" + credentials + ", projectId=" + projectId + ", encodedKey=" + encodedKey
				+ "]";
	}

}
