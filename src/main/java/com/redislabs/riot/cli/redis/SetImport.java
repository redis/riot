package com.redislabs.riot.cli.redis;

import com.redislabs.riot.redis.writer.AbstractCollectionRedisItemWriter;
import com.redislabs.riot.redis.writer.SetWriter;

import picocli.CommandLine.Command;

@Command(name = "set", description = "Set data structure")
public class SetImport extends AbstractCollectionImport {

	@Override
	protected AbstractCollectionRedisItemWriter collectionRedisItemWriter() {
		return new SetWriter();
	}

	@Override
	protected String getDataStructure() {
		return "set";
	}

}
