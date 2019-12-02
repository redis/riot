package com.redislabs.riot.cli.redis.command;

import com.redislabs.riot.batch.redis.writer.map.AbstractCollectionMapWriter;

import picocli.CommandLine.Option;

@SuppressWarnings("rawtypes")
public abstract class AbstractCollectionRedisCommand extends AbstractKeyRedisCommand {

	@Option(names = "--members", arity = "1..*", description = "Member fields for collections: list geo set zset", paramLabel = "<names>")
	private String[] memberFields = new String[0];

	@Override
	protected AbstractCollectionMapWriter keyWriter() {
		AbstractCollectionMapWriter writer = collectionWriter();
		writer.memberFields(memberFields);
		return writer;
	}

	protected abstract AbstractCollectionMapWriter collectionWriter();

}
