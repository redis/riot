package com.redislabs.recharge.redis;

import java.util.Map;

import org.apache.commons.pool2.impl.GenericObjectPool;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.StatefulRediSearchConnection;

import io.lettuce.core.RedisFuture;

@SuppressWarnings("rawtypes")
public abstract class CollectionRedisWriter<T extends CollectionRedisConfiguration>
		extends PipelineRedisWriter<T> {

	public CollectionRedisWriter(T config,
			GenericObjectPool<StatefulRediSearchConnection<String, String>> pool) {
		super(config, pool);
	}

	@Override
	protected RedisFuture<?> write(String id, Map record, RediSearchAsyncCommands<String, String> commands) {
		String member = getValues(record, config.getFields());
		return write(getKey(id), member, record, commands);
	}

	protected abstract RedisFuture<?> write(String key, String member, Map record,
			RediSearchAsyncCommands<String, String> commands);

}