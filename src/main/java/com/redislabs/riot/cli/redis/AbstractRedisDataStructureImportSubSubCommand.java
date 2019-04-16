package com.redislabs.riot.cli.redis;

import com.redislabs.riot.cli.AbstractImportSubSubCommand;
import com.redislabs.riot.redis.writer.AbstractRedisDataStructureWriter;

import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

public abstract class AbstractRedisDataStructureImportSubSubCommand extends AbstractImportSubSubCommand {

	@Parameters(description = "Redis keyspace prefix.")
	private String keyspace;
	@Option(arity = "1..*", names = { "-k", "--keys" }, description = "Key fields.", order = 3)
	private String[] keys = new String[0];

	@Override
	public String getTargetDescription() {
		return getDataStructure() + " \"" + getKeyspaceDescription() + "\"";
	}

	protected abstract String getDataStructure();

	private String getKeyspaceDescription() {
		String description = keyspace;
		for (String key : keys) {
			description += ":<" + key + ">";
		}
		return description;
	}

	@Override
	protected AbstractRedisDataStructureWriter redisWriter() {
		AbstractRedisDataStructureWriter writer = doCreateWriter();
		writer.setKeyspace(keyspace);
		writer.setKeys(keys);
		return writer;
	}

	protected abstract AbstractRedisDataStructureWriter doCreateWriter();

}
