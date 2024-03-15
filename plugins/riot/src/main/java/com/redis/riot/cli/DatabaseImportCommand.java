package com.redis.riot.cli;

import com.redis.riot.core.AbstractImport;
import com.redis.riot.db.DatabaseImport;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Command(name = "db-import", description = "Import from a relational database.")
public class DatabaseImportCommand extends AbstractImportCommand {

	@Parameters(arity = "1", description = "SQL SELECT statement", paramLabel = "SQL")
	String sql;

	@ArgGroup(exclusive = false)
	DatabaseImportArgs args = new DatabaseImportArgs();

	@Override
	protected AbstractImport importRunnable() {
		DatabaseImport runnable = new DatabaseImport();
		runnable.setSql(sql);
		runnable.setDataSourceOptions(args.dataSourceOptions());
		runnable.setFetchSize(args.fetchSize);
		runnable.setMaxItemCount(args.maxItemCount);
		runnable.setMaxResultSetRows(args.maxResultSetRows);
		return runnable;
	}

}
