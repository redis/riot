
package com.redislabs.recharge.redis.writers;

import java.util.List;
import java.util.Map;

import com.redislabs.lettusearch.RediSearchCommands;
import com.redislabs.recharge.RechargeConfiguration.AbstractRedisWriterConfiguration;

@SuppressWarnings("rawtypes")
public abstract class AbstractSyncRedisWriter<T extends AbstractRedisWriterConfiguration>
		extends AbstractRedisWriter<T> {

	protected AbstractSyncRedisWriter(T config) {
		super(config);
	}

	@Override
	public void write(List<? extends Map> records) {
		records.forEach(record -> write(getKey(record), record, connection.sync()));
	}

	protected abstract void write(String key, Map record, RediSearchCommands<String, String> commands);

}
