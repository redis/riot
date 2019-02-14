
package com.redislabs.recharge.redis.writers;

import java.util.List;
import java.util.Map;

import com.redislabs.lettusearch.RediSearchCommands;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.recharge.RechargeConfiguration.AbstractRedisConfiguration;

@SuppressWarnings("rawtypes")
public abstract class AbstractSyncRedisWriter<T extends AbstractRedisConfiguration>
		extends AbstractRedisWriter<T> {

	protected AbstractSyncRedisWriter(T config) {
		super(config);
	}

	@Override
	public void write(List<? extends Map> records) throws Exception {
		StatefulRediSearchConnection<String, String> connection = getConnection();
		try {
		for (Map record : records) {
			write(getKey(record), record, connection.sync());
		}
		} finally {
			release(connection);
		}
	}

	protected abstract void write(String key, Map record, RediSearchCommands<String, String> commands);

}
