package com.redis.riot;

import picocli.CommandLine.ArgGroup;

public abstract class AbstractRedisImportCommand extends AbstractImportCommand {

	@ArgGroup(exclusive = false)
	private RedisArgs redisArgs = new RedisArgs();

	@Override
	protected RedisContext targetRedisContext() {
		return redisArgs.redisContext();
	}

	public RedisArgs getRedisArgs() {
		return redisArgs;
	}

	public void setRedisArgs(RedisArgs clientArgs) {
		this.redisArgs = clientArgs;
	}

}
