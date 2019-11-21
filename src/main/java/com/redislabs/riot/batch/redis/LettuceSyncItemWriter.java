package com.redislabs.riot.batch.redis;

import java.util.List;

import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.sync.BaseRedisCommands;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LettuceSyncItemWriter<K, V, C extends StatefulConnection<K, V>, R extends BaseRedisCommands<K, V>, O>
		extends AbstractLettuceItemWriter<K, V, C, R, O> {

	private RedisWriter<R, O> writer;

	public LettuceSyncItemWriter(LettuceConnector<K, V, C, R> connector, RedisWriter<R, O> writer) {
		super(connector);
		this.writer = writer;
	}

	@Override
	protected void write(List<? extends O> items, R commands) {
		for (O item : items) {
			try {
				writer.write(commands, item);
			} catch (Exception e) {
				if (log.isDebugEnabled()) {
					log.debug("Could not write record {}", item, e);
				} else {
					log.error("Could not write record: {}", e.getMessage());
				}
			}
		}
	}

}
