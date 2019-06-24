package com.redislabs.riot.cli;

import lombok.Data;
import picocli.CommandLine.Option;

@Data
public class RedisKeyOptions {

	@Option(names = "--separator", description = "Redis key separator.", paramLabel = "<string>")
	private String separator = ":";
	@Option(names = "--keyspace", description = "Redis keyspace prefix.", paramLabel = "<string>")
	private String space;
	@Option(names = "--keys", arity = "1..*", description = "Key fields.", paramLabel = "<names>")
	private String[] names = new String[0];
}
