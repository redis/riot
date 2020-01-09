package com.redislabs.riot.cli.redis.command;

import com.redislabs.picocliredis.HelpCommand;
import com.redislabs.riot.cli.ImportCommand;
import com.redislabs.riot.redis.writer.AbstractRedisWriter;

import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

@SuppressWarnings("rawtypes")
@Command
public abstract class AbstractRedisCommand extends HelpCommand implements Runnable {

	@ParentCommand
	private ImportCommand parent;

	@Override
	public void run() {
		parent.execute(redisWriter());
	}

	protected abstract AbstractRedisWriter redisWriter();

}
