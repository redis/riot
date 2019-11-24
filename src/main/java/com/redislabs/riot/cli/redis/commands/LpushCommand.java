package com.redislabs.riot.cli.redis.commands;

import com.redislabs.riot.batch.redis.writer.Lpush;

import picocli.CommandLine.Command;

@Command(name = "lpush", description = "Insert values at the head of a list")
public class LpushCommand extends AbstractCollectionRedisCommand {

	@SuppressWarnings("rawtypes")
	@Override
	protected Lpush collectionWriter() {
		return new Lpush();
	}

}
