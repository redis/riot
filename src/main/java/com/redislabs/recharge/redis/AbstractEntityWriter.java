package com.redislabs.recharge.redis;

import java.util.Map;
import java.util.Map.Entry;

import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.redislabs.recharge.RechargeConfiguration.EntityConfiguration;
import com.redislabs.recharge.redis.key.AbstractKeyBuilder;
import com.redislabs.recharge.redis.key.KeyBuilder;

public abstract class AbstractEntityWriter extends AbstractTemplateWriter {

	private KeyBuilder keyBuilder;

	protected AbstractEntityWriter(StringRedisTemplate template, Entry<String, EntityConfiguration> entity) {
		super(template);
		this.keyBuilder = AbstractKeyBuilder.getKeyBuilder(entity.getKey(), entity.getValue().getKeys());
	}

	protected String getId(Map<String, Object> record) {
		return keyBuilder.getId(record);
	}

	@Override
	protected void write(StringRedisConnection conn, Map<String, Object> record) {
		write(conn, getKey(record), record);
	}

	protected String getKey(Map<String, Object> record) {
		return keyBuilder.getKey(record);
	}

	protected abstract void write(StringRedisConnection conn, String key, Map<String, Object> record);

}
