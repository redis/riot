package com.redislabs.riot.cli;

import picocli.CommandLine.Option;

public class RedisKeyOptions {

	@Option(names = "--separator", description = "Redis key separator", paramLabel = "<string>")
	private String separator = ":";
	@Option(names = "--keyspace", description = "Redis keyspace prefix", paramLabel = "<string>")
	private String space;
	@Option(names = "--keys", arity = "1..*", description = "Key fields", paramLabel = "<names>")
	private String[] fields = new String[0];

	public String getSeparator() {
		return separator;
	}

	public String getSpace() {
		return space;
	}

	public String[] getFields() {
		return fields;
	}

}
