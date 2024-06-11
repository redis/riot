package com.redis.riot;

import com.redis.riot.RedisClientBuilder.RedisURIClient;

import picocli.CommandLine.ArgGroup;

public abstract class AbstractRedisArgsCommand extends AbstractRedisCommand {

	@ArgGroup(exclusive = false)
	private RedisArgs redisArgs = new RedisArgs();

	@Override
	protected RedisURIClient redisURIClient() {
		return redisArgs.configure(redisClientBuilder()).build();
	}

	public RedisArgs getRedisArgs() {
		return redisArgs;
	}

	public void setRedisArgs(RedisArgs args) {
		this.redisArgs = args;
	}

}
