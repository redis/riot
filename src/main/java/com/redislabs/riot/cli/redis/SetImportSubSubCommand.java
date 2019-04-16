package com.redislabs.riot.cli.redis;

import com.redislabs.riot.redis.writer.SetWriter;

import picocli.CommandLine.Command;

@Command(name = "set", description = "Set data structure")
public class SetImportSubSubCommand extends AbstractRedisCollectionImportSubSubCommand {

	@Override
	protected SetWriter doCreateWriter() {
		return new SetWriter();
	}

	@Override
	protected String getDataStructure() {
		return "set";
	}

}
