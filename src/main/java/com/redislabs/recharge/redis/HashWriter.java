package com.redislabs.recharge.redis;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.redislabs.recharge.RechargeConfiguration.EntityConfiguration;

public class HashWriter extends AbstractEntityWriter {

	private ConversionService converter = new DefaultConversionService();

	public HashWriter(StringRedisTemplate template, Entry<String, EntityConfiguration> entity) {
		super(template, entity);
	}

	@Override
	protected void write(StringRedisConnection conn, String key, Map<String, Object> entity) {
		conn.hMSet(key, getValues(entity));
	}

	private Map<String, String> getValues(Map<String, Object> entity) {
		Map<String, String> values = new HashMap<String, String>();
		entity.entrySet().forEach(entry -> values.put(entry.getKey(), convert(entry.getValue())));
		return values;
	}

	private String convert(Object value) {
		return converter.convert(value, String.class);
	}

}
