package com.redislabs.riot.cli.redis;

import com.redislabs.riot.redis.writer.SetWriter;

import picocli.CommandLine.Command;

@Command(name = "set", description = "Redis set data structure")
public class SetWriterCommand extends AbstractCollectionWriterCommand {

	@Override
	protected SetWriter collectionWriter() {
		return new SetWriter();
	}
}
