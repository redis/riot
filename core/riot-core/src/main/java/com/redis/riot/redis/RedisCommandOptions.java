package com.redis.riot.redis;

import com.redis.riot.convert.IdConverterBuilder;

import picocli.CommandLine.Option;

public class RedisCommandOptions {

	@Option(names = { "-s",
			"--separator" }, description = "Key separator (default: ${DEFAULT-VALUE})", paramLabel = "<str>")
	private String keySeparator = IdConverterBuilder.DEFAULT_SEPARATOR;
	@Option(names = { "-r", "--remove" }, description = "Remove key or member fields the first time they are used")
	private boolean removeFields;
	@Option(names = "--ignore-missing", description = "Ignore missing fields")
	private boolean ignoreMissingFields;

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
