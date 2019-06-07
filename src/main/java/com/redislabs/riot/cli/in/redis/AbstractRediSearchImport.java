package com.redislabs.riot.cli.in.redis;

import com.redislabs.riot.cli.RedisDriver;
import com.redislabs.riot.cli.in.AbstractImportWriterCommand;
import com.redislabs.riot.redis.writer.RedisItemWriter;
import com.redislabs.riot.redis.writer.search.AbstractRediSearchItemWriter;

import lombok.Getter;
import picocli.CommandLine.Option;

public abstract class AbstractRediSearchImport extends AbstractImportWriterCommand {

	@Option(names = "--keyspace", description = "Document keyspace prefix.")
	private String keyspace;
	@Option(names = "--keys", required = true, arity = "1..*", description = "Document key fields.")
	private String[] keys = new String[0];
	@Getter
	@Option(names = "--index", description = "Name of the RediSearch index", required = true)
	private String index;

	@Override
	protected String getKeyspace() {
		return keyspace;
	}

	@Override
	protected String[] getKeys() {
		return keys;
	}

	@Override
	protected RedisDriver getDriver() {
		return RedisDriver.Lettuce;
	}

	@Override
	protected RedisItemWriter redisItemWriter() {
		AbstractRediSearchItemWriter writer = rediSearchItemWriter();
		writer.setIndex(index);
		return writer;
	}

	protected abstract AbstractRediSearchItemWriter rediSearchItemWriter();

	@Override
	protected String getKeyspaceDescription() {
		return index;
	}

}
