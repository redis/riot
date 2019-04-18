package com.redislabs.riot.cli.redis;

import com.redislabs.riot.redis.writer.AbstractCollectionRedisItemWriter;

import picocli.CommandLine.Option;

public abstract class AbstractCollectionRedisImportSubSubCommand extends AbstractRedisImportSubSubCommand {

	@Option(arity = "1..*", names = "--fields", description = "Fields used to build member ids for collection data structures (list, set, zset, geo).")
	private String[] fields;

	@Override
	protected AbstractCollectionRedisItemWriter redisItemWriter() {
		AbstractCollectionRedisItemWriter writer = collectionRedisItemWriter();
		writer.setFields(fields);
		return writer;
	}

	protected abstract AbstractCollectionRedisItemWriter collectionRedisItemWriter();

}
