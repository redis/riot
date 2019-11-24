package com.redislabs.riot.cli.file;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.nio.file.Path;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.cloud.aws.core.io.s3.SimpleStorageProtocolResolver;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.WritableResource;
import org.springframework.util.Assert;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.redislabs.riot.batch.file.OutputStreamResource;

import lombok.Data;
import lombok.Getter;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

@Data
public class FileOptions implements AWSCredentialsProvider {

	public enum FileType {
		json, csv, fixed
	}

	static class ResourceOptions {

		@Getter
		@Option(names = "--file", description = "File path")
		private Path file;

		@Getter
		@Option(names = "--url", description = "File URL")
		private URI url;

		public boolean isUri() {
			return url != null;
		}

		public String path() {
			if (isUri()) {
				return url.toString();
			}
			return file.toString();
		}

		public boolean isGzip() {
			return path().toLowerCase().endsWith(".gz");
		}

		public FileType type() {
			String path = path().toLowerCase();
			if (path.toLowerCase().endsWith(".json") || path.endsWith(".json.gz")) {
				return FileType.json;
			}
			return FileType.csv;
		}

	}

	@ArgGroup(exclusive = true, multiplicity = "1")
	private ResourceOptions resource = new ResourceOptions();
	@Option(names = { "-z", "--gzip" }, description = "File is gzip compressed")
	private boolean gzip;
	@Option(names = { "-t", "--filetype" }, description = "File type: ${COMPLETION-CANDIDATES}", paramLabel = "<type>")
	private FileType type;
	@Option(names = { "-e",
			"--encoding" }, description = "File encoding (default: ${DEFAULT-VALUE})", paramLabel = "<charset>")
	private String encoding = FlatFileItemWriter.DEFAULT_CHARSET;
	@Option(names = "--s3-access", description = "AWS S3 access key ID", paramLabel = "<string>")
	private String accessKey;
	@Option(names = "--s3-secret", arity = "0..1", interactive = true, description = "AWS S3 secret access key", paramLabel = "<string>")
	private String secretKey;
	@Option(names = "--s3-region", description = "AWS region", paramLabel = "<string>")
	private String region;

	@Override
	public AWSCredentials getCredentials() {
		return new BasicAWSCredentials(accessKey, secretKey);
	}

	@Override
	public void refresh() {
		// do nothing
	}

	public Resource resource() throws MalformedURLException {
		if (resource.isUri()) {
			URI uri = resource.getUrl();
			if (uri.getScheme().equals("s3")) {
				AmazonS3 s3 = AmazonS3Client.builder().withCredentials(this).withRegion(region).build();
				SimpleStorageProtocolResolver resolver = new SimpleStorageProtocolResolver(s3);
				resolver.afterPropertiesSet();
				return resolver.resolve(uri.toString(), new DefaultResourceLoader());
			}
			return new UncustomizedUrlResource(uri);
		}
		return new FileSystemResource(resource.getFile());
	}

	public FileType type() {
		if (type == null) {
			return resource.type();
		}
		return type;
	}

	public Resource inputResource() throws IOException {
		Resource resource = resource();
		if (isGzip()) {
			return new InputStreamResource(new GZIPInputStream(resource.getInputStream()), resource.getDescription());
		}
		return resource;
	}

	public WritableResource outputResource() throws IOException {
		Resource resource = resource();
		Assert.isInstanceOf(WritableResource.class, resource);
		WritableResource writable = (WritableResource) resource;
		if (isGzip()) {
			return new OutputStreamResource(new GZIPOutputStream(writable.getOutputStream()),
					writable.getDescription());
		}
		return writable;
	}

	private boolean isGzip() {
		return gzip || resource.isGzip();
	}

}
