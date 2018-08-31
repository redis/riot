package com.redislabs.recharge.redis;

import java.util.Map;

import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.redislabs.recharge.RechargeConfiguration.EntityConfiguration;

public abstract class AbstractEntityWriter extends AbstractRedisWriter {

	public AbstractEntityWriter(StringRedisTemplate template, EntityConfiguration entity) {
		super(template, entity.getName(), entity.getKeys());
	}

	@Override
	protected void write(StringRedisConnection conn, String keyspace, String id, Map<String, Object> fields) {
		write(conn, String.join(KEY_SEPARATOR, keyspace, id), fields);
	}

	protected abstract void write(StringRedisConnection conn, String key, Map<String, Object> fields);

}
