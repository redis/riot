package com.redislabs.riot.cli;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.zip.GZIPInputStream;

import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;

import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

public class FileOptions {

	static enum FileType {
		json, csv, fixed
	}

	@Parameters(paramLabel = "PATH", description = "Path to file (URL or local file)")
	private String path;
	@Option(names = "--gz", description = "Input is gzip compressed")
	private boolean gzip;
	@Option(names = "--type", description = "File type: ${COMPLETION-CANDIDATES}", paramLabel = "<type>")
	private FileType type;

	public Resource resource() throws IOException {
		Resource resource = resource(URI.create(path));
		if (gzip || path.toLowerCase().endsWith(".gz")) {
			return new InputStreamResource(new GZIPInputStream(resource.getInputStream()));
		}
		return resource;
	}

	private Resource resource(URI uri) throws MalformedURLException {
		if (uri.isAbsolute()) {
			return new UrlResource(uri);
		}
		return new FileSystemResource(uri.toString());
	}

	public FileType fileType() throws IOException {
		if (type == null) {
			if (path.toLowerCase().endsWith(".json")) {
				return FileType.json;
			}
			return FileType.csv;
		}
		return type;
	}

	public String description() {
		return path;
	}

}