package com.redislabs.riot.cli.file;

import java.net.URI;
import java.nio.file.Path;

import lombok.Data;
import lombok.experimental.Accessors;
import picocli.CommandLine.Option;

@Accessors(fluent = true)
public @Data class ResourceOptions {

	@Option(names = "--file", description = "File path")
	private Path file;

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
			return FileType.Json;
		}
		return FileType.Csv;
	}

}