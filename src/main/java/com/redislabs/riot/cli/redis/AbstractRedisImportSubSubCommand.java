package com.redislabs.riot.cli.redis;

import com.redislabs.riot.cli.in.AbstractImportSubSubCommand;
import com.redislabs.riot.redis.RedisConverter;
import com.redislabs.riot.redis.writer.AbstractRedisItemWriter;

import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

public abstract class AbstractRedisImportSubSubCommand extends AbstractImportSubSubCommand {

	@Parameters(description = "Redis keyspace prefix.")
	private String keyspace;
	@Option(arity = "1..*", names = { "-k", "--keys" }, description = "Key fields.")
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
	protected AbstractRedisItemWriter itemWriter() {
		AbstractRedisItemWriter writer = redisItemWriter();
		RedisConverter converter = new RedisConverter();
		converter.setKeyspace(keyspace);
		converter.setKeys(keys);
		writer.setConverter(converter);
		return writer;
	}

	protected abstract AbstractRedisItemWriter redisItemWriter();

}
