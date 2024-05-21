package com.redis.riot.cli;

import com.redis.riot.db.DatabaseExport;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "db-export", description = "Export Redis data to a relational database.")
public class DbExportCommand extends AbstractExportCommand {

	@Parameters(arity = "1", description = "SQL INSERT statement.", paramLabel = "SQL")
	private String sql;

	@ArgGroup(exclusive = false)
	private DbArgs dbArgs = new DbArgs();

	@ArgGroup(exclusive = false)
	private KeyValueMapProcessorArgs mapProcessorArgs = new KeyValueMapProcessorArgs();

	@Option(names = "--assert-updates", defaultValue = "true", fallbackValue = "true", description = "Confirm every insert results in update of at least one row. True by default.", negatable = true)
	private boolean assertUpdates = DatabaseExport.DEFAULT_ASSERT_UPDATES;

	@Override
	protected DatabaseExport exportCallable() {
		DatabaseExport callable = new DatabaseExport();
		callable.setSql(sql);
		callable.setAssertUpdates(assertUpdates);
		callable.setDataSourceOptions(dbArgs.dataSourceOptions());
		callable.setMapProcessorOptions(mapProcessorArgs.keyValueMapProcessorOptions());
		return callable;
	}

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	public DbArgs getDbArgs() {
		return dbArgs;
	}

	public void setDbArgs(DbArgs dbArgs) {
		this.dbArgs = dbArgs;
	}

	public boolean isAssertUpdates() {
		return assertUpdates;
	}

	public void setAssertUpdates(boolean assertUpdates) {
		this.assertUpdates = assertUpdates;
	}

	public KeyValueMapProcessorArgs getMapProcessorArgs() {
		return mapProcessorArgs;
	}

	public void setMapProcessorArgs(KeyValueMapProcessorArgs args) {
		this.mapProcessorArgs = args;
	}

}
