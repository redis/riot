package com.redislabs.riot.cli.redis.command;

import com.redislabs.riot.redis.writer.map.Lpush;

import picocli.CommandLine.Command;

@Command(name = "lpush", description = "Insert values at the head of a list")
public class LpushCommand extends AbstractCollectionRedisCommand {

	@SuppressWarnings("rawtypes")
	@Override
	protected Lpush collectionWriter() {
		return new Lpush();
	}

}
