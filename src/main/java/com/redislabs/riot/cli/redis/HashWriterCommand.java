package com.redislabs.riot.cli.redis;

import com.redislabs.riot.redis.writer.HashWriter;

import picocli.CommandLine.Command;

@Command(name = "hash", description = "Redis hash data structure")
public class HashWriterCommand extends AbstractDataStructureWriterCommand {

	public HashWriter writer() {
		return new HashWriter();
	}

}
