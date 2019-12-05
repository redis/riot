package com.redislabs.riot.cli.db;

import java.util.Map;

import org.springframework.batch.item.database.JdbcCursorItemReader;

import com.redislabs.riot.batch.TransferContext;
import com.redislabs.riot.cli.MapImportCommand;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "db-import", description = "Import from a database")
public class DatabaseImportCommand extends MapImportCommand {

	@ArgGroup(exclusive = false, heading = "Database options%n", order = 3)
	DatabaseReaderOptions options = new DatabaseReaderOptions();

	@Override
	protected JdbcCursorItemReader<Map<String, Object>> reader(TransferContext context) throws Exception {
		return options.reader();
	}
}
