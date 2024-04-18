package com.redis.riot.cli;

import com.redis.riot.db.DatabaseExport;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Command(name = "db-export", description = "Export Redis data to a relational database.")
public class DatabaseExportCommand extends AbstractExportCommand {

	@Parameters(arity = "1", description = "SQL INSERT statement.", paramLabel = "SQL")
	String sql;

	@ArgGroup(exclusive = false)
	DatabaseExportArgs args = new DatabaseExportArgs();

	@Override
	protected DatabaseExport exportRunnable() {
		DatabaseExport runnable = new DatabaseExport();
		runnable.setSql(sql);
		runnable.setAssertUpdates(args.assertUpdates);
		runnable.setDataSourceOptions(args.dataSourceOptions());
		runnable.setKeyRegex(args.keyRegex);
		return runnable;
	}

}
