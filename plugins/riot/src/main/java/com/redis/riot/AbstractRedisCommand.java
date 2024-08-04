package com.redis.riot;

import com.redis.riot.core.AbstractJobCommand;

import io.lettuce.core.RedisURI;
import picocli.CommandLine.ArgGroup;

public abstract class AbstractRedisCommand<C extends RedisExecutionContext> extends AbstractJobCommand<C> {

	@ArgGroup(exclusive = false)
	private RedisURIArgs redisURIArgs = new RedisURIArgs();

	@ArgGroup(exclusive = false)
	private RedisClientArgs redisClientArgs = new RedisClientArgs();

	@Override
	protected C executionContext() throws Exception {
		C context = super.executionContext();
		context.setRedisContext(redisContext());
		return context;
	}

	private RedisContext redisContext() {
		RedisURI redisURI = redisURIArgs.redisURI();
		log.info("Creating Redis context with URI {}, cluster {} and {}", redisURI, redisClientArgs);
		RedisContext redisContext = new RedisContext();
		redisContext.setUri(redisURI);
		redisContext.setCluster(redisClientArgs.isCluster());
		redisContext.setClientOptions(redisClientArgs.clientOptions());
		redisContext.setPoolSize(redisClientArgs.getPoolSize());
		return redisContext;
	}

	public RedisURIArgs getRedisURIArgs() {
		return redisURIArgs;
	}

	public void setRedisURIArgs(RedisURIArgs argfs) {
		this.redisURIArgs = argfs;
	}

	public RedisClientArgs getRedisClientArgs() {
		return redisClientArgs;
	}

	public void setRedisClientArgs(RedisClientArgs clientArgs) {
		this.redisClientArgs = clientArgs;
	}

}
