package com.redislabs.riot.cli.db;

import java.util.Map;

import org.springframework.batch.item.database.JdbcCursorItemReader;

import com.redislabs.riot.cli.MapImportCommand;

import lombok.Data;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "db-import", description = "Import from a database")
public @Data class DatabaseImportCommand extends MapImportCommand {

	@ArgGroup(exclusive = false, heading = "Database options%n", order = 3)
	private DatabaseReaderOptions options = new DatabaseReaderOptions();

	@Override
	protected JdbcCursorItemReader<Map<String, Object>> reader() throws Exception {
		return options.reader();
	}

}
