package com.redislabs.riot.cli.redis;

import com.redislabs.riot.redis.writer.HashWriter;

import picocli.CommandLine.Command;

@Command(name = "hash", description = "Hash data structure")
public class HashImportSubSubCommand extends AbstractRedisDataStructureImportSubSubCommand {

	@Override
	protected HashWriter doCreateWriter() {
		return new HashWriter();
	}

	@Override
	protected String getDataStructure() {
		return "hashes";
	}

}
