package com.redislabs.riot.cli.redis;

import com.redislabs.riot.redis.writer.HashWriter;
import com.redislabs.riot.redis.writer.RedisItemWriter;

import picocli.CommandLine.Command;

@Command(name = "hash", description = "Hash data structure")
public class HashImport extends AbstractSingleImport {

	@Override
	protected RedisItemWriter redisItemWriter() {
		return new HashWriter();
	}

	@Override
	protected String getDataStructure() {
		return "hashes";
	}

}
