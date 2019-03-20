package com.redislabs.recharge.redis;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;

import io.lettuce.core.RedisFuture;

@SuppressWarnings("rawtypes")
public abstract class RedisCommandWriter<T extends RedisCommandConfiguration> extends PipelineRedisWriter {

	protected T config;

	protected RedisCommandWriter(T config) {
		this.config = config;
	}

	@Override
	protected RedisFuture<?> write(Map record, RediSearchAsyncCommands<String, String> commands) {
		String id = getValues(record, config.getKeys());
		return write(id, record, commands);
	}

	protected abstract RedisFuture<?> write(String id, Map record, RediSearchAsyncCommands<String, String> commands);

	protected String getKey(String id) {
		if (id == null) {
			return config.getKeyspace();
		}
		return join(config.getKeyspace(), id);
	}

}
