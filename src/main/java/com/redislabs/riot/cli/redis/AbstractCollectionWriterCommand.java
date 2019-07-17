package com.redislabs.riot.cli.redis;

import com.redislabs.riot.redis.writer.AbstractCollectionRedisItemWriter;

import picocli.CommandLine.Option;

public abstract class AbstractCollectionWriterCommand extends AbstractDataStructureWriterCommand {

	@Option(names = "--fields", arity = "1..*", description = "Fields used to build member ids for collections (list, set, zset, geo)")
	private String[] fields = new String[0];

	@Override
	protected AbstractCollectionRedisItemWriter writer() {
		AbstractCollectionRedisItemWriter collectionWriter = collectionWriter();
		collectionWriter.setFields(fields);
		return collectionWriter;
	}

	protected abstract AbstractCollectionRedisItemWriter collectionWriter();
}
