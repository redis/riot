package com.redislabs.riot.cli;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;

import com.redislabs.riot.cli.db.DatabaseImportOptions;
import com.redislabs.riot.cli.file.FileImportOptions;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "import", description = "Import data into Redis")
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

	@SuppressWarnings("rawtypes")
	@Override
	protected List<ItemProcessor> processors() {
		List<ItemProcessor> processors = new ArrayList<>();
		processors.addAll(super.processors());
		if (file.isSet()) {
			processors.add(file.postProcessor());
		}
		return processors;
	}

	@Override
	protected String taskName() {
		return "Importing";
	}

}
