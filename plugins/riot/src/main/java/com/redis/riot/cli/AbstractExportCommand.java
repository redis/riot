package com.redis.riot.cli;

import com.redis.riot.core.AbstractExport;

import picocli.CommandLine.ArgGroup;

public abstract class AbstractExportCommand extends AbstractRiotCommand {

	@ArgGroup(exclusive = false, heading = "Redis client options%n")
	private RedisClientArgs redisClientArgs = new RedisClientArgs();

	@ArgGroup(exclusive = false, heading = "Redis reader options%n")
	private RedisReaderArgs redisReaderArgs = new RedisReaderArgs();

	@ArgGroup(exclusive = false, heading = "Processor options%n")
	private KeyValueProcessorArgs processorArgs = new KeyValueProcessorArgs();

	@Override
	protected AbstractExport callable() {
		AbstractExport export = exportCallable();
		export.setRedisClientOptions(redisClientArgs.redisClientOptions());
		export.setReaderOptions(redisReaderArgs.redisReaderOptions());
		export.setProcessorOptions(processorArgs.processorOptions());
		return export;
	}

	protected abstract AbstractExport exportCallable();

	public RedisClientArgs getRedisClientArgs() {
		return redisClientArgs;
	}

	public void setRedisClientArgs(RedisClientArgs redisClientArgs) {
		this.redisClientArgs = redisClientArgs;
	}

	public RedisReaderArgs getRedisReaderArgs() {
		return redisReaderArgs;
	}

	public void setRedisReaderArgs(RedisReaderArgs redisReaderArgs) {
		this.redisReaderArgs = redisReaderArgs;
	}

	public KeyValueProcessorArgs getProcessorArgs() {
		return processorArgs;
	}

	public void setProcessorArgs(KeyValueProcessorArgs args) {
		this.processorArgs = args;
	}

}
