package com.redis.riot.cli;

import com.redis.riot.core.AbstractImport;

import picocli.CommandLine.ArgGroup;

public abstract class AbstractStructImportCommand extends AbstractRiotCommand {

	@ArgGroup(exclusive = false)
	RedisWriterArgs writerArgs = new RedisWriterArgs();

	@Override
	protected AbstractImport runnable() {
		AbstractImport runnable = importRunnable();
		runnable.setWriterOptions(writerArgs.writerOptions());
		return runnable;
	}

	protected abstract AbstractImport importRunnable();

}
