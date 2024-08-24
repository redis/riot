package com.redis.riot;

import com.redis.riot.core.AbstractJobCommand;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.RedisItemWriter;

import picocli.CommandLine.ArgGroup;

public abstract class AbstractRedisCommand extends AbstractJobCommand {

	@ArgGroup(exclusive = false)
	private RedisArgs redisArgs = new RedisArgs();

	protected RedisContext redisContext;

	@Override
	protected void execute() throws Exception {
		redisContext = redisArgs.redisContext();
		try {
			super.execute();
		} finally {
			redisContext.close();
		}
	}

	protected void configure(RedisItemReader<?, ?, ?> reader) {
		configureAsyncReader(reader);
		redisContext.configure(reader);
	}

	protected void configure(RedisItemWriter<?, ?, ?> writer) {
		redisContext.configure(writer);
	}

	public RedisArgs getRedisArgs() {
		return redisArgs;
	}

	public void setRedisArgs(RedisArgs clientArgs) {
		this.redisArgs = clientArgs;
	}

}
