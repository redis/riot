package com.redislabs.riot.cli.redis;

import com.redislabs.riot.cli.AbstractImportSubSubCommand;

import picocli.CommandLine.Option;

public abstract class AbstractSingleRedisImportSubSubCommand extends AbstractImportSubSubCommand {

	@Option(names = "--keyspace", required = true, description = "Redis keyspace prefix.")
	private String keyspace;
	@Option(names = "--keys", arity = "1..*", required = true, description = "Key fields.")
	private String[] keys = new String[0];

	@Override
	protected String getKeyspace() {
		return keyspace;
	}

	@Override
	protected String[] getKeys() {
		return keys;
	}

}
