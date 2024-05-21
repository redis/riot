package com.redis.riot.cli;

import com.redis.riot.core.AbstractRedisCallable;

import picocli.CommandLine.ArgGroup;

public abstract class AbstractRedisCommand extends AbstractRiotCommand {

	@ArgGroup(exclusive = false, heading = "Redis options%n")
	private RedisArgs redisArgs = new RedisArgs();

	@Override
	protected AbstractRedisCallable callable() {
		AbstractRedisCallable callable = redisCallable();
		callable.setRedisURI(redisArgs.redisURI());
		callable.setRedisClientOptions(redisArgs.clientOptions());
		return callable;
	}

	protected abstract AbstractRedisCallable redisCallable();

	public RedisArgs getRedisArgs() {
		return redisArgs;
	}

	public void setRedisArgs(RedisArgs args) {
		this.redisArgs = args;
	}

}
