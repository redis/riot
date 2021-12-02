package com.redis.riot.file;

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

import com.redis.riot.file.resource.FilenameInputStreamResource;
import com.redis.riot.file.resource.OutputStreamResource;
import com.redis.riot.file.resource.UncustomizedUrlResource;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public class FileOptions {

	public static final Charset DEFAULT_ENCODING = Charset.defaultCharset();

	@Option(names = "--encoding", description = "File encoding (default: ${DEFAULT-VALUE})", paramLabel = "<charset>")
	private Charset encoding = DEFAULT_ENCODING;
	@Option(names = { "-z", "--gzip" }, description = "File is gzip compressed")
	private boolean gzip;
	@ArgGroup(exclusive = false, heading = "Amazon Simple Storage Service options%n")
	private S3Options s3 = new S3Options();
	@ArgGroup(exclusive = false, heading = "Google Cloud Storage options%n")
	private GcsOptions gcs = new GcsOptions();

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

	@SuppressWarnings("unchecked")
	public static class FileOptionsBuilder<B extends FileOptionsBuilder<B>> {

		protected Charset encoding = DEFAULT_ENCODING;
		protected boolean gzip;
		protected S3Options s3;
		protected GcsOptions gcs;

		public B encoding(Charset encoding) {
			Assert.notNull(encoding, "Encoding must not be null");
			this.encoding = encoding;
			return (B) this;
		}

		public B gzip(boolean gzip) {
			this.gzip = gzip;
			return (B) this;
		}

		public B s3(S3Options s3) {
			this.s3 = s3;
			return (B) this;
		}

		public B gcs(GcsOptions gcs) {
			this.gcs = gcs;
			return (B) this;
		}

		protected <T extends FileOptions> T build(T options) {
			options.setEncoding(encoding);
			options.setGcs(gcs);
			options.setGzip(gzip);
			options.setS3(s3);
			return options;
		}

	}

	public Resource inputResource(String file) throws IOException {
		if (FileUtils.isConsole(file)) {
			return new FilenameInputStreamResource(System.in, "stdin", "Standard Input");
		}
		Resource resource = resource(file, true);
		if (gzip || FileUtils.isGzip(file)) {
			GZIPInputStream gzipInputStream = new GZIPInputStream(resource.getInputStream());
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

}
