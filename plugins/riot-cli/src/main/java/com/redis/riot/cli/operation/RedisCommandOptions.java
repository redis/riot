package com.redis.riot.cli.operation;

import com.redis.riot.core.convert.IdConverterBuilder;

import picocli.CommandLine.Option;

public class RedisCommandOptions {

	public static final String DEFAULT_SEPARATOR = IdConverterBuilder.DEFAULT_SEPARATOR;
	public static final boolean DEFAULT_REMOVE_FIELDS = false;
	public static final boolean DEFAULT_IGNORE_MISSING_FIELDS = false;

	@Option(names = { "-s",
			"--separator" }, description = "Key separator (default: ${DEFAULT-VALUE}).", paramLabel = "<str>")
	private String keySeparator = DEFAULT_SEPARATOR;

	@Option(names = { "-r", "--remove" }, description = "Remove key or member fields the first time they are used.")
	private boolean removeFields = DEFAULT_REMOVE_FIELDS;

	@Option(names = "--ignore-missing", description = "Ignore missing fields.")
	private boolean ignoreMissingFields = DEFAULT_IGNORE_MISSING_FIELDS;

	public String getKeySeparator() {
		return keySeparator;
	}

	public void setKeySeparator(String keySeparator) {
		this.keySeparator = keySeparator;
	}

	public boolean isRemoveFields() {
		return removeFields;
	}

	public void setRemoveFields(boolean removeFields) {
		this.removeFields = removeFields;
	}

	public boolean isIgnoreMissingFields() {
		return ignoreMissingFields;
	}

	public void setIgnoreMissingFields(boolean ignoreMissingFields) {
		this.ignoreMissingFields = ignoreMissingFields;
	}

}
