package com.redislabs.recharge.redis;

import java.util.HashMap;
import java.util.Map;

import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.redislabs.recharge.RechargeConfiguration.RedisWriterConfiguration;

public class HMSetWriter extends AbstractRedisWriter {

	public HMSetWriter(StringRedisTemplate template, RedisWriterConfiguration entity) {
		super(template, entity);
	}

	@Override
	protected void write(StringRedisConnection conn, String key, Map<String, Object> fields) {
		Map<String, String> values = new HashMap<String, String>();
		fields.entrySet().forEach(entry -> values.put(entry.getKey(), convert(entry.getValue(), String.class)));
		conn.hMSet(key, values);
	}

}