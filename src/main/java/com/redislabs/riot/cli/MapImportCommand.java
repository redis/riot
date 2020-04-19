package com.redislabs.riot.cli;

import java.util.Arrays;
import java.util.Map;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.support.CompositeItemProcessor;

import com.redislabs.riot.cli.db.DatabaseImportOptions;
import com.redislabs.riot.cli.file.FileImportOptions;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "import", description = "Import data into Redis", sortOptions = false)
public class MapImportCommand extends ImportCommand {

	@ArgGroup(exclusive = false, heading = "File options%n")
	private FileImportOptions file = new FileImportOptions();
	@ArgGroup(exclusive = false, heading = "Database options%n")
	private DatabaseImportOptions db = new DatabaseImportOptions();

	@Override
	protected ItemReader<Map<String, Object>> reader() throws Exception {
		if (db.getUrl() != null) {
			return db.reader();
		}
		return file.reader();
	}

	@Override
	protected ItemProcessor<Map<String, Object>, Map<String, Object>> processor() throws Exception {
		ItemProcessor<Map<String, Object>, Map<String, Object>> processor = super.processor();
		if (file.isSet()) {
			ItemProcessor<Map<String, Object>, Map<String, Object>> fileProcessor = file.processor();
			if (processor == null) {
				return fileProcessor;
			}
			if (fileProcessor == null) {
				return processor;
			}
			CompositeItemProcessor<Map<String, Object>, Map<String, Object>> composite = new CompositeItemProcessor<>();
			composite.setDelegates(Arrays.asList(processor, fileProcessor));
			return composite;
		}
		return processor;
	}

	@Override
	protected String getMainFlowName() {
		return "import";
	}

	@Override
	protected String taskName() {
		return "Importing";
	}

}
