package com.redislabs.riot.cli.redis;

import com.redislabs.riot.cli.in.AbstractImportSubSubCommand;
import com.redislabs.riot.redis.RedisConverter;
import com.redislabs.riot.redis.writer.AbstractRedisItemWriter;

import picocli.CommandLine.Option;

public abstract class AbstractRedisImportSubSubCommand extends AbstractImportSubSubCommand {

	@Option(names = { "-s", "--keyspace" }, description = "Redis keyspace prefix.")
	private String keyspace;
	@Option(names = { "-k", "--keys" }, arity = "1..*", description = "Key fields.")
	private String[] keys = new String[0];

	@Override
	public String getTargetDescription() {
		return getDataStructure() + " \"" + getKeyspaceDescription() + "\"";
	}

	protected abstract String getDataStructure();

	protected String getKeyspaceDescription() {
		String description = keyspace == null ? "" : keyspace;
		for (String key : keys) {
			description += ":<" + key + ">";
		}
		return description;
	}

	@Override
	protected AbstractRedisItemWriter itemWriter() {
		AbstractRedisItemWriter writer = redisItemWriter();
		writer.setConverter(redisConverter());
		return writer;
	}

	protected RedisConverter redisConverter() {
		RedisConverter converter = new RedisConverter();
		converter.setKeyspace(keyspace);
		converter.setKeys(keys);
		return converter;
	}

	protected abstract AbstractRedisItemWriter redisItemWriter();

}
