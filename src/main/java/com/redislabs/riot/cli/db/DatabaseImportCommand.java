package com.redislabs.riot.cli.db;

import java.util.Map;

import org.springframework.batch.item.ItemReader;

import com.redislabs.riot.cli.ImportCommand;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "db-import", description = "Import database")
public class DatabaseImportCommand extends ImportCommand {

	@ArgGroup(exclusive = false, heading = "Database connection options%n", order = 2)
	private DatabaseOptions db = new DatabaseOptions();
	@ArgGroup(exclusive = false, heading = "Database reader options%n", order = 3)
	private DatabaseReaderOptions reader = new DatabaseReaderOptions();

	@Override
	protected ItemReader<Map<String, Object>> reader() throws Exception {
		return reader.reader(db.dataSource());
	}

}
