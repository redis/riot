package com.redis.riot;

import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

import io.lettuce.core.api.sync.RedisCommands;

public class PingExecutionItemReader extends AbstractItemCountingItemStreamItemReader<PingExecution> {

	private final RedisCommands<String, String> redisCommands;

	public PingExecutionItemReader(RedisCommands<String, String> redisCommands) {
		setName(ClassUtils.getShortName(getClass()));
		this.redisCommands = redisCommands;
	}

	@Override
	protected void doOpen() throws Exception {
		// do nothing
	}

	@Override
	protected void doClose() throws Exception {
		// do nothing
	}

	@Override
	protected PingExecution doRead() throws Exception {
		return new PingExecution().reply(redisCommands.ping());
	}

}