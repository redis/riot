package com.redis.riot.cli;

import java.util.regex.Pattern;

import com.redis.riot.core.AbstractMapExport;
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

	@Option(names = "--key-regex", description = "Regex for key-field extraction (default: ${DEFAULT-VALUE}).", paramLabel = "<str>")
	private Pattern keyRegex = AbstractMapExport.DEFAULT_KEY_REGEX;

	@Option(names = "--no-assert-updates", description = "Confirm every insert results in update of at least one row. True by default.", negatable = true)
	private boolean assertUpdates = DatabaseExport.DEFAULT_ASSERT_UPDATES;

	@Override
	protected DatabaseExport exportRunnable() {
		DatabaseExport runnable = new DatabaseExport();
		runnable.setSql(sql);
		runnable.setAssertUpdates(assertUpdates);
		runnable.setDataSourceOptions(dbArgs.dataSourceOptions());
		runnable.setKeyRegex(keyRegex);
		return runnable;
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

	public Pattern getKeyRegex() {
		return keyRegex;
	}

	public void setKeyRegex(Pattern keyRegex) {
		this.keyRegex = keyRegex;
	}

	public boolean isAssertUpdates() {
		return assertUpdates;
	}

	public void setAssertUpdates(boolean assertUpdates) {
		this.assertUpdates = assertUpdates;
	}

}
