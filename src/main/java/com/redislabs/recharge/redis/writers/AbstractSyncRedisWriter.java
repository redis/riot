
package com.redislabs.recharge.redis.writers;

import org.springframework.batch.item.ExecutionContext;

import com.redislabs.lettusearch.RediSearchCommands;
import com.redislabs.recharge.RechargeConfiguration.AbstractRedisWriterConfiguration;

public abstract class AbstractSyncRedisWriter<T extends AbstractRedisWriterConfiguration>
		extends AbstractRedisWriter<T> {

	RediSearchCommands<String, String> commands;

	protected AbstractSyncRedisWriter(T config) {
		super(config);
	}

	@Override
	public void open(ExecutionContext executionContext) {
		commands = connection.sync();
		super.open(executionContext);
	}

}
