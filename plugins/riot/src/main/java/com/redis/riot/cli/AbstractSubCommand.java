package com.redis.riot.cli;

import com.redis.riot.core.AbstractRunnable;

import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

@Command
public abstract class AbstractSubCommand extends BaseCommand implements Runnable {

	@ParentCommand
	protected AbstractMainCommand parent;

	@Override
	public void run() {
		AbstractRunnable runnable = runnable();
		runnable.setRedisClientOptions(parent.redisArgs.redisOptions());
		runnable.run();
	}

	protected abstract AbstractRunnable runnable();

}
