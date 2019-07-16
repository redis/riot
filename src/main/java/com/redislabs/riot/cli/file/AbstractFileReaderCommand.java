package com.redislabs.riot.cli.file;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.zip.GZIPInputStream;

import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;

import com.redislabs.riot.cli.AbstractReaderCommand;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public abstract class AbstractFileReaderCommand extends AbstractReaderCommand {

	@ArgGroup(exclusive = true, multiplicity = "1")
	private InputOptions input = new InputOptions();

	@Option(names = "--gz", description = "Input is gzip compressed.")
	private boolean gzip;

	static class InputOptions {
		@Option(names = "--file", description = "Path to input file.", paramLabel = "<path>")
		File file;
		@Option(names = "--url", description = "URL for input file.")
		URL url;

		@Override
		public String toString() {
			if (file == null) {
				if (url == null) {
					return "not set";
				}
				return url.toString();
			}
			return file.toString();
		}

		public boolean isURL() {
			return url != null;
		}
	}

	@Override
	public String getSourceDescription() {
		return "file " + input.toString();
	}

	private Resource resource(Resource resource) throws IOException {
		if (gzip) {
			return new InputStreamResource(new GZIPInputStream(resource.getInputStream()));
		}
		return resource;
	}

	protected Resource resource() throws IOException {
		if (input.isURL()) {
			return resource(new UrlResource(input.url));
		}
		return resource(new FileSystemResource(input.file));
	}

}
