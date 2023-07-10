package com.redis.riot.cli.file;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.WritableResource;
import org.springframework.util.Assert;
import org.springframework.util.ResourceUtils;

import com.redis.riot.core.FileUtils;
import com.redis.riot.core.RuntimeIOException;
import com.redis.riot.core.resource.FilenameInputStreamResource;
import com.redis.riot.core.resource.OutputStreamResource;
import com.redis.riot.core.resource.UncustomizedUrlResource;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public class FileOptions {

	public static final Charset DEFAULT_ENCODING = Charset.defaultCharset();
	public static final boolean DEFAULT_GZIP = false;

	@Option(names = "--encoding", description = "File encoding (default: ${DEFAULT-VALUE}).", paramLabel = "<charset>")
	protected Charset encoding = DEFAULT_ENCODING;
	@Option(names = { "-z", "--gzip" }, description = "File is gzip compressed.")
	protected boolean gzip = DEFAULT_GZIP;
	@ArgGroup(exclusive = false, heading = "Amazon Simple Storage Service options%n")
	protected S3Options s3 = new S3Options();
	@ArgGroup(exclusive = false, heading = "Google Cloud Storage options%n")
	protected GcsOptions gcs = new GcsOptions();

	public FileOptions() {
	}

	protected FileOptions(Builder<?> builder) {
		this.encoding = builder.encoding;
		this.gzip = builder.gzip;
		this.s3 = builder.s3;
		this.gcs = builder.gcs;
	}

	public Charset getEncoding() {
		return encoding;
	}

	public void setEncoding(Charset encoding) {
		this.encoding = encoding;
	}

	public boolean isGzip() {
		return gzip;
	}

	public void setGzip(boolean gzip) {
		this.gzip = gzip;
	}

	public S3Options getS3() {
		return s3;
	}

	public void setS3(S3Options s3) {
		this.s3 = s3;
	}

	public GcsOptions getGcs() {
		return gcs;
	}

	public void setGcs(GcsOptions gcs) {
		this.gcs = gcs;
	}

	public Resource inputResource(String file) {
		if (FileUtils.isConsole(file)) {
			return new FilenameInputStreamResource(System.in, "stdin", "Standard Input");
		}
		Resource resource;
		try {
			resource = resource(file, true);
		} catch (IOException e) {
			throw new RuntimeIOException("Could not read file " + file, e);
		}
		Assert.isTrue(resource.exists(), "Input resource must exist: " + file);
		if (gzip || FileUtils.isGzip(file)) {
			GZIPInputStream gzipInputStream;
			try {
				gzipInputStream = new GZIPInputStream(resource.getInputStream());
			} catch (IOException e) {
				throw new RuntimeIOException("Could not open input stream", e);
			}
			return new FilenameInputStreamResource(gzipInputStream, resource.getFilename(), resource.getDescription());
		}
		return resource;
	}

	public WritableResource outputResource(String file) throws IOException {
		if (FileUtils.isConsole(file)) {
			return new OutputStreamResource(System.out, "stdout", "Standard Output");
		}
		Resource resource = resource(file, false);
		Assert.isInstanceOf(WritableResource.class, resource);
		WritableResource writableResource = (WritableResource) resource;
		if (gzip || FileUtils.isGzip(file)) {
			OutputStream outputStream = writableResource.getOutputStream();
			return new OutputStreamResource(new GZIPOutputStream(outputStream), resource.getFilename(),
					resource.getDescription());
		}
		return writableResource;
	}

	private Resource resource(String location, boolean readOnly) throws IOException {
		if (FileUtils.isS3(location)) {
			return s3.resource(location);
		}
		if (FileUtils.isGcs(location)) {
			return gcs.resource(location, readOnly);
		}
		if (ResourceUtils.isUrl(location)) {
			return new UncustomizedUrlResource(location);
		}
		return new FileSystemResource(location);
	}

	public static class Builder<B extends Builder<B>> {

		private Charset encoding = DEFAULT_ENCODING;
		private boolean gzip;
		private S3Options s3 = new S3Options();
		private GcsOptions gcs = new GcsOptions();

		@SuppressWarnings("unchecked")
		public B encoding(Charset encoding) {
			this.encoding = encoding;
			return (B) this;
		}

		@SuppressWarnings("unchecked")
		public B gzip(boolean gzip) {
			this.gzip = gzip;
			return (B) this;
		}

		@SuppressWarnings("unchecked")
		public B s3(S3Options s3) {
			this.s3 = s3;
			return (B) this;
		}

		@SuppressWarnings("unchecked")
		public B gcs(GcsOptions gcs) {
			this.gcs = gcs;
			return (B) this;
		}

	}

}
