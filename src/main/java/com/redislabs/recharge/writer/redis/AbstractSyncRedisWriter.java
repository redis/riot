
package com.redislabs.recharge.writer.redis;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.RediSearchCommands;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.recharge.RechargeConfiguration.RedisWriterConfiguration;

public abstract class AbstractSyncRedisWriter extends AbstractRedisWriter {

	RediSearchCommands<String, String> commands;

	protected AbstractSyncRedisWriter(RediSearchClient client, RedisWriterConfiguration config) {
		super(client, config);
	}

	@Override
	protected void open(StatefulRediSearchConnection<String, String> connection) {
		commands = connection.sync();
		doOpen();
	}

	protected void doOpen() {
	}

	@Override
	public synchronized void close() {
		commands = null;
		super.close();
	}

}
