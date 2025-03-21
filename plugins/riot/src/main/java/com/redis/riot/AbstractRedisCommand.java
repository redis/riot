package com.redis.riot;

import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.riot.core.AbstractJobCommand;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.RedisItemWriter;

import picocli.CommandLine.ArgGroup;

public abstract class AbstractRedisCommand extends AbstractJobCommand {

	@ArgGroup(exclusive = false, heading = "Redis options%n")
	private RedisArgs redisArgs = new RedisArgs();

	private RedisContext redisContext;

	@Override
	protected void initialize() {
		super.initialize();
		redisContext = RedisContext.of(redisArgs);
		redisContext.afterPropertiesSet();
	}

	@Override
	protected void teardown() {
		if (redisContext != null) {
			redisContext.close();
		}
		super.teardown();
	}

	protected RedisModulesCommands<String, String> commands() {
		return redisContext.getConnection().sync();
	}

	protected void configure(RedisItemReader<?, ?> reader) {
		configureAsyncStreamSupport(reader);
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
