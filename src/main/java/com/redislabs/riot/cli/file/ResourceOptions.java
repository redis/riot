package com.redislabs.riot.cli.file;

import java.net.URI;
import java.nio.file.Path;

import lombok.Data;
import picocli.CommandLine.Option;

public @Data class ResourceOptions {

	@Option(names = "--file", description = "File path")
	private Path file;
	@Option(names = "--url", description = "File URL")
	private URI url;

	public boolean uri() {
		return url != null;
	}

	public String path() {
		if (uri()) {
			return url.toString();
		}
		return file.toString();
	}

	public boolean gzip() {
		return path().toLowerCase().endsWith(".gz");
	}

	public FileType type() {
		String path = path().toLowerCase();
		if (path.toLowerCase().endsWith(".json") || path.endsWith(".json.gz")) {
			return FileType.Json;
		}
		if (path.toLowerCase().endsWith(".xml") || path.endsWith(".xml.gz")) {
			return FileType.Xml;
		}
		return FileType.Csv;
	}

}