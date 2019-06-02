package com.redislabs.riot.cli.redis;

import com.redislabs.riot.redis.writer.AbstractRedisItemWriter;
import com.redislabs.riot.redis.writer.HashWriter;

import picocli.CommandLine.Command;

@Command(name = "hash", description = "Hash data structure")
public class HashImportSubSubCommand extends AbstractSingleRedisImportSubSubCommand {

	@Override
	protected AbstractRedisItemWriter redisItemWriter() {
		return new HashWriter();
	}

	@Override
	protected String getDataStructure() {
		return "hashes";
	}

}
