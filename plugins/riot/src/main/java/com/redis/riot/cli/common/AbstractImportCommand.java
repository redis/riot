package com.redis.riot.cli.common;

import com.redis.spring.batch.RedisItemWriter.BaseBuilder;

import picocli.CommandLine.ArgGroup;

public abstract class AbstractImportCommand extends AbstractCommand {

	@ArgGroup(exclusive = false, heading = "Redis operation options%n")
	protected RedisOperationOptions operationOptions = new RedisOperationOptions();

	public RedisOperationOptions getOperationOptions() {
		return operationOptions;
	}

	public void setOperationOptions(RedisOperationOptions options) {
		this.operationOptions = options;
	}

	protected <B extends BaseBuilder<?, ?, B>> B configure(B builder) {
		return builder.operationOptions(operationOptions.writeOperationOptions());
	}

}
