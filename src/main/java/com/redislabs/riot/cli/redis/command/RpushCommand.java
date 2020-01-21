package com.redislabs.riot.cli.redis.command;

import com.redislabs.riot.redis.writer.map.Rpush;

import picocli.CommandLine.Command;

@Command(name = "rpush", description = "Insert values at the tail of a list")
public class RpushCommand extends AbstractCollectionRedisCommand {

	@Override
	protected Rpush collectionWriter() {
		return new Rpush();
	}

}
