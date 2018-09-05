package com.redislabs.recharge.redis;

import java.util.Map;

import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.redislabs.recharge.RechargeConfiguration.EntityConfiguration;

public abstract class AbstractEntityWriter extends AbstractRedisWriter {

	private EntityConfiguration config;

	public AbstractEntityWriter(StringRedisTemplate template, EntityConfiguration entity) {
		super(template, entity.getName(), entity.getKeys());
		this.config = entity;
	}
	
	protected EntityConfiguration getConfig() {
		return config;
	}

	@Override
	protected void write(StringRedisConnection conn, String keyspace, String id, Map<String, Object> fields) {
		write(conn, join(keyspace, id), fields);
	}

	protected abstract void write(StringRedisConnection conn, String key, Map<String, Object> fields);

}
