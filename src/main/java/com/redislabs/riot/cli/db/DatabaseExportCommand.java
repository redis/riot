package com.redislabs.riot.cli.db;

import java.util.Map;

import org.springframework.batch.item.ItemWriter;

import com.redislabs.riot.cli.HashExportCommand;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "db-export", description = "Export to database")
public class DatabaseExportCommand extends HashExportCommand {

	@ArgGroup(exclusive = false, heading = "Database writer options%n", order = 3)
	private DatabaseWriterOptions options = new DatabaseWriterOptions();

	@Override
	protected ItemWriter<Map<String, Object>> writer() throws Exception {
		return options.writer();
	}

	@Override
	protected String taskName() {
		return "Exporting to " + options.url();
	}

	@Override
	protected String unitName() {
		return "row";
	}

}
