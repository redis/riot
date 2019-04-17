package com.redislabs.riot.cli.redis;

import com.redislabs.riot.redis.writer.AbstractRedisCollectionWriter;

import picocli.CommandLine.Option;

public abstract class AbstractRedisCollectionImportSubSubCommand extends AbstractRedisDataStructureImportSubSubCommand {

	@Option(arity = "1..*", names = "--fields", description = "Fields used to build member ids for collection data structures (list, set, zset, geo).")
	private String[] fields;

	@Override
	protected AbstractRedisCollectionWriter redisWriter() {
		AbstractRedisCollectionWriter writer = (AbstractRedisCollectionWriter) super.redisWriter();
		writer.setFields(fields);
		return writer;
	}

}
