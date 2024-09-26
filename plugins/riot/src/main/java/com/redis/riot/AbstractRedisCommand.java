package com.redis.riot;

import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.riot.core.AbstractJobCommand;
import com.redis.spring.batch.item.redis.RedisItemWriter;

import picocli.CommandLine.ArgGroup;

public abstract class AbstractRedisCommand extends AbstractJobCommand {

	@ArgGroup(exclusive = false)
	private RedisArgs redisArgs = new RedisArgs();

	private RedisContext redisContext;

	@Override
	protected void execute() throws Exception {
		redisContext = RedisContext.create(redisArgs.redisURI(), redisArgs.isCluster(), redisArgs.getProtocolVersion(),
				redisArgs.getSslArgs());
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

	public RedisArgs getRedisArgs() {
		return redisArgs;
	}

	public void setRedisArgs(RedisArgs clientArgs) {
		this.redisArgs = clientArgs;
	}

}
