package com.redislabs.riot.cli.redis;

import com.redislabs.riot.cli.in.AbstractImportSubSubCommand;
import com.redislabs.riot.redis.RedisConverter;
import com.redislabs.riot.redis.writer.AbstractRedisItemWriter;

public abstract class AbstractRedisImportSubSubCommand extends AbstractImportSubSubCommand {

	@Override
	public String getTargetDescription() {
		return getDataStructure() + " \"" + getKeyspaceDescription() + "\"";
	}

	protected abstract String getDataStructure();

	protected String getKeyspaceDescription() {
		String description = getKeyspace() == null ? "" : getKeyspace();
		for (String key : getKeys()) {
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
		converter.setKeyspace(getKeyspace());
		converter.setKeys(getKeys());
		return converter;
	}

	protected abstract AbstractRedisItemWriter redisItemWriter();

	protected abstract String getKeyspace();

	protected abstract String[] getKeys();

}
