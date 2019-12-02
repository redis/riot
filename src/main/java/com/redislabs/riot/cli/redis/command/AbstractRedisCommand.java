package com.redislabs.riot.cli.redis.command;

import com.redislabs.picocliredis.HelpCommand;
import com.redislabs.riot.batch.redis.writer.AbstractRedisWriter;
import com.redislabs.riot.cli.ImportCommand;

import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.ParentCommand;
import picocli.CommandLine.Spec;

@SuppressWarnings("rawtypes")
@Command
public abstract class AbstractRedisCommand extends HelpCommand implements Runnable {

	@ParentCommand
	private ImportCommand parent;
	@Spec
	private CommandSpec spec;

	@Override
	public void run() {
		parent.execute(redisWriter());
	}

	protected abstract AbstractRedisWriter redisWriter();

}
