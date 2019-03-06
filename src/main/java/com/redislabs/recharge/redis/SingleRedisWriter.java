package com.redislabs.recharge.redis;

import java.util.Map;

import org.apache.commons.pool2.impl.GenericObjectPool;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.StatefulRediSearchConnection;

import io.lettuce.core.RedisFuture;

@SuppressWarnings("rawtypes")
public abstract class SingleRedisWriter<T extends DataStructureConfiguration>
		extends PipelineRedisWriter<T> {

	public SingleRedisWriter(T config, GenericObjectPool<StatefulRediSearchConnection<String, String>> pool) {
		super(config, pool);
	}

	@Override
	protected RedisFuture<?> write(String id, Map record, RediSearchAsyncCommands<String, String> commands) {
		String key = getKey(id);
		return writeSingle(key, record, commands);
	}

	protected abstract RedisFuture<?> writeSingle(String key, Map record,
			RediSearchAsyncCommands<String, String> commands);

}