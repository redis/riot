package com.redislabs.riot.cli.in.redis;

import com.redislabs.riot.cli.in.AbstractRedisImportWriterCommand;
import com.redislabs.riot.redis.writer.AbstractCollectionRedisItemWriter;

import picocli.CommandLine.Option;

public abstract class AbstractCollectionImport extends AbstractRedisImportWriterCommand {

	@Option(names = "--keyspace", required = true, description = "Redis keyspace prefix.")
	private String keyspace;
	@Option(names = "--keys", arity = "1..*", description = "Key fields.")
	private String[] keys = new String[0];
	@Option(names = "--fields", required = true, arity = "1..*", description = "Fields used to build member ids for collection data structures (list, set, zset, geo).")
	private String[] fields;

	@Override
	protected String getKeyspace() {
		return keyspace;
	}

	@Override
	protected String[] getKeys() {
		return keys;
	}

	@Override
	protected AbstractCollectionRedisItemWriter redisItemWriter() {
		AbstractCollectionRedisItemWriter writer = collectionRedisItemWriter();
		writer.setFields(fields);
		return writer;
	}

	protected abstract AbstractCollectionRedisItemWriter collectionRedisItemWriter();

}
