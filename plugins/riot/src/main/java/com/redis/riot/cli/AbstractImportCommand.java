package com.redis.riot.cli;

import com.redis.riot.core.AbstractImport;

import picocli.CommandLine.ArgGroup;

public abstract class AbstractImportCommand extends AbstractRiotCommand {

	@ArgGroup(exclusive = false, heading = "Redis client options%n")
	private RedisClientArgs redisClientArgs = new RedisClientArgs();

	@ArgGroup(exclusive = false, heading = "Redis writer options%n")
	private RedisWriterArgs redisWriterArgs = new RedisWriterArgs();

	@Override
	protected AbstractImport callable() {
		AbstractImport callable = importCallable();
		callable.setRedisClientOptions(redisClientArgs.redisClientOptions());
		callable.setWriterOptions(redisWriterArgs.writerOptions());
		return callable;
	}

	protected abstract AbstractImport importCallable();

	public RedisClientArgs getRedisClientArgs() {
		return redisClientArgs;
	}

	public void setRedisClientArgs(RedisClientArgs redisClientArgs) {
		this.redisClientArgs = redisClientArgs;
	}

	public RedisWriterArgs getRedisWriterArgs() {
		return redisWriterArgs;
	}

	public void setRedisWriterArgs(RedisWriterArgs redisWriterArgs) {
		this.redisWriterArgs = redisWriterArgs;
	}

}
