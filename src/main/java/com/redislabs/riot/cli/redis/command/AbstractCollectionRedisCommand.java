package com.redislabs.riot.cli.redis.command;

import com.redislabs.riot.redis.writer.KeyBuilder;
import com.redislabs.riot.redis.writer.map.AbstractCollectionMapWriter;

import picocli.CommandLine.Option;

public abstract class AbstractCollectionRedisCommand extends AbstractKeyRedisCommand {

	@Option(names = "--member-keyspace", description = "Keyspace prefix for member IDs", paramLabel = "<str>")
	private String memberKeyspace;
	@Option(names = "--members", arity = "1..*", description = "Fields composing member IDs for collections: list geo set zset", paramLabel = "<names>")
	private String[] memberFields = new String[0];

	@Override
	protected AbstractCollectionMapWriter keyWriter() {
		AbstractCollectionMapWriter writer = collectionWriter();
		writer.memberIdBuilder(
				KeyBuilder.builder().separator(separator()).prefix(memberKeyspace).fields(memberFields).build());
		return writer;
	}

	protected abstract AbstractCollectionMapWriter collectionWriter();

}
