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

import lombok.Data;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public abstract class AbstractFileReaderCommand extends AbstractReaderCommand {

	@ArgGroup(exclusive = true, multiplicity = "1")
	private InputOptions input = new InputOptions();

	@Option(names = "--gz", description = "Input is gzip compressed.")
	private boolean gzip;

	@Data
	static class InputOptions {
		@Option(names = "--file", description = "Path to input file.", paramLabel = "<path>")
		private File file;
		@Option(names = "--url", description = "URL for input file.")
		private URL url;
	}

	@Override
	public String getSourceDescription() {
		return "file " + (input.getFile() == null ? input.getUrl() : input.getFile());
	}

	private Resource resource(Resource resource) throws IOException {
		if (gzip) {
			return new InputStreamResource(new GZIPInputStream(resource.getInputStream()));
		}
		return resource;
	}

	protected Resource resource() throws IOException {
		if (input.getUrl() != null) {
			return resource(new UrlResource(input.getUrl()));
		}
		return resource(new FileSystemResource(input.getFile()));
	}

}
