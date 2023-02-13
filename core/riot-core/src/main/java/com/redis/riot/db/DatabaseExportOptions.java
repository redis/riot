package com.redis.riot.db;

import picocli.CommandLine.Option;

public class DatabaseExportOptions {

	@Option(names = "--key-regex", description = "Regex for key-field extraction (default: ${DEFAULT-VALUE}).", paramLabel = "<str>")
	private String keyRegex = "\\w+:(?<id>.+)";
	@Option(names = "--no-assert-updates", description = "Confirm every insert results in update of at least one row. True by default.", negatable = true)
	private boolean assertUpdates = true;

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
