package com.redis.riot;

import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.riot.core.AbstractJobCommand;
import com.redis.spring.batch.item.redis.RedisItemWriter;

import picocli.CommandLine.ArgGroup;

public abstract class AbstractRedisCommand extends AbstractJobCommand {

	@ArgGroup(exclusive = false)
	private SimpleRedisArgs redisArgs = new SimpleRedisArgs();

	private RedisContext redisContext;

	@Override
	protected void execute() throws Exception {
		redisContext = redisArgs.redisContext();
		try {
			super.execute();
		} finally {
			redisContext.close();
		}
	}

	protected RedisModulesCommands<String, String> commands() {
		return redisContext.getConnection().sync();
	}

	protected void configure(RedisItemWriter<?, ?, ?> writer) {
		redisContext.configure(writer);
	}

	public SimpleRedisArgs getRedisArgs() {
		return redisArgs;
	}

	public void setRedisArgs(SimpleRedisArgs clientArgs) {
		this.redisArgs = clientArgs;
	}

}
