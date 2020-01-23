package com.redislabs.riot.cli.file;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.WritableResource;
import org.springframework.util.Assert;

import com.redislabs.riot.file.OutputStreamResource;

import lombok.Data;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public @Data class FileOptions {

	@ArgGroup(exclusive = true, multiplicity = "1")
	private ResourceOptions resourceOptions = new ResourceOptions();
	@Option(names = { "-z", "--gzip" }, description = "File is gzip compressed")
	private boolean gzip;
	@Option(names = { "-t", "--filetype" }, description = "File type: ${COMPLETION-CANDIDATES}", paramLabel = "<type>")
	private FileType type;
	@Option(names = "--s3-access", description = "AWS S3 access key ID", paramLabel = "<string>")
	private String accessKey;
	@Option(names = "--s3-secret", arity = "0..1", interactive = true, description = "AWS S3 secret access key", paramLabel = "<string>")
	private String secretKey;
	@Option(names = "--s3-region", description = "AWS region", paramLabel = "<string>")
	private String region;

	public Resource resource() throws MalformedURLException {
		if (resourceOptions.uri()) {
			URI uri = resourceOptions.url();
			if (uri.getScheme().equals("s3")) {
				return S3ResourceBuilder.resource(accessKey, secretKey, region, uri);
			}
			return new UncustomizedUrlResource(uri);
		}
		return new FileSystemResource(resourceOptions.file());
	}

	public FileType type() {
		if (type == null) {
			return resourceOptions.type();
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
		return gzip || resourceOptions.gzip();
	}

}
