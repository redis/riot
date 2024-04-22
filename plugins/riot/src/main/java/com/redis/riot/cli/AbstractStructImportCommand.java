package com.redis.riot.cli;

import com.redis.riot.core.AbstractStructImport;

import picocli.CommandLine.ArgGroup;

public abstract class AbstractStructImportCommand extends AbstractRiotCommand {

	@ArgGroup(exclusive = false, heading = "Writer options%n")
	RedisWriterArgs writerArgs = new RedisWriterArgs();

	@Override
	protected AbstractStructImport runnable() {
		AbstractStructImport runnable = importRunnable();
		runnable.setWriterOptions(writerArgs.writerOptions());
		return runnable;
	}

	protected abstract AbstractStructImport importRunnable();

}
