package com.redis.riot.cli.common;

import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

public class DbExportOptions {

	public static final boolean DEFAULT_ASSERT_UPDATES = true;
	public static final String DEFAULT_KEY_REGEX = "\\w+:(?<id>.+)";

	@Mixin
	private DataSourceOptions dataSourceOptions = new DataSourceOptions();
	@Option(names = "--key-regex", description = "Regex for key-field extraction (default: ${DEFAULT-VALUE}).", paramLabel = "<str>")
	private String keyRegex = DEFAULT_KEY_REGEX;
	@Option(names = "--no-assert-updates", description = "Confirm every insert results in update of at least one row. True by default.", negatable = true)
	private boolean assertUpdates = DEFAULT_ASSERT_UPDATES;

	public DataSourceOptions getDataSourceOptions() {
		return dataSourceOptions;
	}

	public void setDataSourceOptions(DataSourceOptions dataSourceOptions) {
		this.dataSourceOptions = dataSourceOptions;
	}

	public boolean isAssertUpdates() {
		return assertUpdates;
	}

	public void setAssertUpdates(boolean assertUpdates) {
		this.assertUpdates = assertUpdates;
	}

	public String getKeyRegex() {
		return keyRegex;
	}

	public void setKeyRegex(String keyRegex) {
		this.keyRegex = keyRegex;
	}

	@Override
	public String toString() {
		return "DatabaseExportOptions [keyRegex=" + keyRegex + ", assertUpdates=" + assertUpdates + "]";
	}

}
