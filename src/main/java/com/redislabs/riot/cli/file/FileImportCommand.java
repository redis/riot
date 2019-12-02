package com.redislabs.riot.cli.file;

import java.util.Map;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;

import com.redislabs.riot.cli.MapImportCommand;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "file-import", description = "Import from a file")
public class FileImportCommand extends MapImportCommand {

	@ArgGroup(exclusive = false, heading = "File options%n", order = 3)
	FileReaderOptions options = new FileReaderOptions();

	@Override
	protected ItemReader<Map<String, Object>> reader() throws Exception {
		return options.reader();
	}

	@Override
	protected ItemProcessor<Map<String, Object>, Map<String, Object>> postProcessor() {
		return options.postProcessor();
	}

}
