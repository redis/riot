package com.redislabs.recharge.redis;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.redislabs.recharge.config.KeyConfiguration;

public class HashWriter extends AbstractTemplateWriter {

	public HashWriter(KeyConfiguration keyConfig, StringRedisTemplate template) {
		super(keyConfig, template);
	}

	private ConversionService conversionService = new DefaultConversionService();

	@Override
	protected void write(StringRedisConnection conn, Map<String, Object> map) {
		Map<String, String> values = new HashMap<String, String>();
		for (Entry<String, Object> entry : map.entrySet()) {
			values.put(entry.getKey(), conversionService.convert(entry.getValue(), String.class));
		}
		conn.hMSet(getKey(map), values);
	}

}
