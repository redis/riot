package com.redislabs.riot.cli.file;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.WritableResource;
import org.springframework.util.Assert;

import com.redislabs.riot.batch.file.OutputStreamResource;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

@Slf4j
@Data
class FileOptions {

	public enum FileType {
		json, csv, fixed
	}

	@Option(required = true, names = "--file", description = "File path")
	private String path;
	@Option(names = { "-z", "--gzip" }, description = "File is gzip compressed")
	private boolean gzip;
	@Option(names = { "-t", "--filetype" }, description = "File type: ${COMPLETION-CANDIDATES}", paramLabel = "<type>")
	private FileType type;
	@Option(names = { "-e",
			"--encoding" }, description = "File encoding (default: ${DEFAULT-VALUE})", paramLabel = "<charset>")
	private String encoding = FlatFileItemWriter.DEFAULT_CHARSET;
	@ArgGroup(exclusive = false, heading = "AWS S3 Options%n", order = 2)
	private S3Options s3Options = new S3Options();

	public FileType type() {
		if (type == null) {
			if (path.toLowerCase().endsWith(".json") || path.toLowerCase().endsWith(".json.gz")) {
				return FileType.json;
			}
			return FileType.csv;
		}
		return type;
	}

	private Resource resource() {
		try {
			URI uri = URI.create(path);
			if (uri.isAbsolute()) {
				return urlResource(uri);
			}
		} catch (Exception e) {
			log.debug("Could not parse URL {}", path, e);
		}
		return new FileSystemResource(path);
	}

	private Resource urlResource(URI uri) throws MalformedURLException {
		// try S3 first
		if (uri.getScheme().equals("s3")) {
			return s3Options.resource(path);
		}
		return new UncustomizedUrlResource(uri);
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
		return gzip || path.toLowerCase().endsWith(".gz");
	}

}
