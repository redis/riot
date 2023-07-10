package com.redis.riot.cli.common;

import com.redis.spring.batch.RedisItemWriter.BaseBuilder;

import picocli.CommandLine.ArgGroup;

public abstract class AbstractImportCommand extends AbstractCommand {

	@ArgGroup(exclusive = false, heading = "Writer options%n")
	protected RedisWriterOptions writerOptions = new RedisWriterOptions();

	public RedisWriterOptions getWriterOptions() {
		return writerOptions;
	}

	public void setWriterOptions(RedisWriterOptions writerOptions) {
		this.writerOptions = writerOptions;
	}

	protected <B extends BaseBuilder<?, ?, B>> B configure(B builder) {
		return builder.options(writerOptions.writerOptions());
	}

}
