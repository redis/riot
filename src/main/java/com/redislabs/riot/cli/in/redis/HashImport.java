package com.redislabs.riot.cli.in.redis;

import com.redislabs.riot.redis.writer.HashWriter;

import picocli.CommandLine.Command;

@Command(name = "hash", description = "Hash data structure")
public class HashImport extends AbstractSingleImport {

	@Override
	protected HashWriter redisItemWriter() {
		return new HashWriter();
	}

	@Override
	protected String getDataStructure() {
		return "hashes";
	}

}
