package com.redislabs.recharge.redis;

import java.util.HashMap;
import java.util.Map;

import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.redislabs.recharge.RechargeConfiguration.EntityConfiguration;

public class HashWriter extends AbstractEntityWriter {

	public HashWriter(StringRedisTemplate template, EntityConfiguration entity) {
		super(template, entity);
	}

	@Override
	protected void write(StringRedisConnection conn, String key, Map<String, Object> fields) {
		Map<String, String> values = new HashMap<String, String>();
		fields.entrySet().forEach(entry -> values.put(entry.getKey(), convert(entry.getValue(), String.class)));
		conn.hMSet(key, values);
	}

}