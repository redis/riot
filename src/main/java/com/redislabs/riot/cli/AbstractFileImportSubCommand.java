package com.redislabs.riot.cli;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Map;

import org.springframework.batch.item.ItemStreamReader;
import org.springframework.core.io.Resource;

import com.redislabs.riot.file.FileConfig;

import picocli.CommandLine.Option;

public abstract class AbstractFileImportSubCommand extends AbstractImportSubCommand {

	@Option(names = { "-f",
			"--file" }, description = "Path to input file. Mutually exclusive with url option.", order = 0)
	private File file;
	@Option(names = { "-u",
			"--url" }, description = "URL for input file. Mutually exclusive with file option.", order = 0)
	private URL url;
	@Option(names = "--gzip", description = "Input is gzip compressed.", order = 9)
	private boolean gzip;

	public File getFile() {
		return file;
	}

	public URL getUrl() {
		return url;
	}

	@Override
	public String getSourceDescription() {
		return "file " + (file == null ? url : file);
	}

	@Override
	public ItemStreamReader<Map<String, Object>> reader() throws IOException {
		Resource resource = resource();
		if (gzip) {
			resource = new FileConfig().gzip(resource);
		}
		return reader(resource);
	}

	private Resource resource() throws IOException {
		if (file == null) {
			if (url == null) {
				throw new IOException("No URL or file specified");
			}
			return new FileConfig().resource(url);
		}
		return new FileConfig().resource(file);
	}

	protected abstract ItemStreamReader<Map<String, Object>> reader(Resource resource) throws IOException;

}
