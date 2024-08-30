package com.redis.riot;

import com.redis.spring.batch.item.redis.RedisItemWriter;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public abstract class AbstractRedisImportCommand extends AbstractImportCommand {

	@ArgGroup(exclusive = false)
	private SimpleRedisArgs redisArgs = new SimpleRedisArgs();

	@Option(names = "--pool", description = "Max number of Redis connections in pool (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	private int poolSize = RedisItemWriter.DEFAULT_POOL_SIZE;

	@Override
	protected void configureTargetRedisWriter(RedisItemWriter<?, ?, ?> writer) {
		super.configureTargetRedisWriter(writer);
		log.info("Configuring Redis writer with poolSize {}", poolSize);
		writer.setPoolSize(poolSize);
	}

	@Override
	protected RedisContext targetRedisContext() {
		return redisArgs.redisContext();
	}

	public SimpleRedisArgs getRedisArgs() {
		return redisArgs;
	}

	public void setRedisArgs(SimpleRedisArgs clientArgs) {
		this.redisArgs = clientArgs;
	}

	public int getPoolSize() {
		return poolSize;
	}

	public void setPoolSize(int poolSize) {
		this.poolSize = poolSize;
	}

}
