package com.redis.riot.cli;

import java.io.File;

import com.redis.riot.file.AmazonS3Options;
import com.redis.riot.file.FileOptions;
import com.redis.riot.file.GoogleStorageOptions;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public class FileArgs {

	@Option(names = "--encoding", description = "File encoding (default: ${DEFAULT-VALUE}).", paramLabel = "<charset>")
	private String encoding = FileOptions.DEFAULT_ENCODING;

	@Option(names = { "-z", "--gzip" }, description = "File is gzip compressed.")
	private boolean gzipped;

	@ArgGroup(exclusive = false)
	private S3Args s3 = new S3Args();

	@ArgGroup(exclusive = false)
	private GcsArgs gcs = new GcsArgs();

	public FileOptions fileOptions() {
		FileOptions options = new FileOptions();
		options.setAmazonS3Options(s3.amazonS3Options());
		options.setEncoding(encoding);
		options.setGoogleStorageOptions(gcs.googleStorageOptions());
		options.setGzipped(gzipped);
		return options;
	}

	public String getEncoding() {
		return encoding;
	}

	public void setEncoding(String encoding) {
		this.encoding = encoding;
	}

	public boolean isGzipped() {
		return gzipped;
	}

	public void setGzipped(boolean gzipped) {
		this.gzipped = gzipped;
	}

	public S3Args getS3() {
		return s3;
	}

	public void setS3(S3Args s3) {
		this.s3 = s3;
	}

	public GcsArgs getGcs() {
		return gcs;
	}

	public void setGcs(GcsArgs gcs) {
		this.gcs = gcs;
	}

	private static class S3Args {

		@Option(names = "--s3-access", description = "Access key.", paramLabel = "<key>")
		private String accessKey;

		@Option(names = "--s3-secret", arity = "0..1", interactive = true, description = "Secret key.", paramLabel = "<key>")
		private String secretKey;

		@Option(names = "--s3-region", description = "AWS region.", paramLabel = "<name>")
		private String region;

		public AmazonS3Options amazonS3Options() {
			AmazonS3Options options = new AmazonS3Options();
			options.setAccessKey(accessKey);
			options.setSecretKey(secretKey);
			options.setRegion(region);
			return options;
		}

	}

	private static class GcsArgs {

		@Option(names = "--gcs-key-file", description = "GCS private key (e.g. /usr/local/key.json).", paramLabel = "<file>")
		private File keyFile;

		@Option(names = "--gcs-project", description = "GCP project id.", paramLabel = "<id>")
		private String projectId;

		@Option(names = "--gcs-key", arity = "0..1", interactive = true, description = "GCS Base64 encoded key.", paramLabel = "<key>")
		private String encodedKey;

		public GoogleStorageOptions googleStorageOptions() {
			GoogleStorageOptions options = new GoogleStorageOptions();
			options.setKeyFile(keyFile);
			options.setProjectId(projectId);
			options.setEncodedKey(encodedKey);
			return options;
		}

	}

}
