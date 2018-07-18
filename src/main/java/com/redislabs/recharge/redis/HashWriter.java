package com.redislabs.recharge.redis;

import java.util.HashMap;
import java.util.Map;

import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.redislabs.recharge.Entity;
import com.redislabs.recharge.RechargeConfiguration.EntityConfiguration;

public class HashWriter extends AbstractTemplateWriter {

	public HashWriter(EntityConfiguration keyConfig, StringRedisTemplate template) {
		super(keyConfig, template);
	}

	@Override
	protected void write(StringRedisConnection conn, Entity entity, String id) {
		conn.hMSet(getKey(entity, id), getValues(entity));
	}

	private Map<String, String> getValues(Entity entity) {
		Map<String, String> values = new HashMap<String, String>();
		for (String field : entity.getFields().keySet()) {
			String stringValue = getValue(entity, field, String.class);
			values.put(field, stringValue);
		}
		return values;
	}

}
