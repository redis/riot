package com.redislabs.riot.cli.in.file;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.zip.GZIPInputStream;

import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;

import com.redislabs.riot.cli.in.AbstractImportReaderCommand;

import picocli.CommandLine.Option;

public abstract class AbstractFileImport extends AbstractImportReaderCommand {

	@Option(names = "--file", description = "Path to input file. Mutually exclusive with url option.")
	private File file;
	@Option(names = "--url", description = "URL for input file. Mutually exclusive with file option.")
	private URL url;
	@Option(names = "--gz", description = "Input is gzip compressed.")
	private boolean gzip;

	@Override
	public String getSourceDescription() {
		return "file " + (file == null ? url : file);
	}

	private Resource resource(Resource resource) throws IOException {
		if (gzip) {
			return new InputStreamResource(new GZIPInputStream(resource.getInputStream()));
		}
		return resource;
	}

	protected Resource resource() throws IOException {
		if (url != null) {
			return resource(new UrlResource(url));
		}
		return resource(new FileSystemResource(file));
	}

}
