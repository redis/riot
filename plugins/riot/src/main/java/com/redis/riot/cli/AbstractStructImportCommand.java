package com.redis.riot.cli;

import com.redis.riot.core.AbstractStructImport;

import picocli.CommandLine.ArgGroup;

public abstract class AbstractStructImportCommand extends AbstractJobCommand {

	@ArgGroup(exclusive = false, heading = "Writer options%n")
	RedisWriterArgs writerArgs = new RedisWriterArgs();

	@Override
	protected AbstractStructImport jobRunnable() {
		AbstractStructImport runnable = importRunnable();
		runnable.setWriterOptions(writerArgs.writerOptions());
		return runnable;
	}

	protected abstract AbstractStructImport importRunnable();

}
