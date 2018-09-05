package com.redislabs.recharge.redis;

import java.util.Map;

import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.redislabs.recharge.RechargeConfiguration.EntityConfiguration;

public class HincrbyWriter extends AbstractEntityWriter {

	public HincrbyWriter(StringRedisTemplate template, EntityConfiguration entity) {
		super(template, entity);
	}

	@Override
	protected void write(StringRedisConnection conn, String key, Map<String, Object> fields) {
		Long delta = convert(fields.get(getConfig().getCommand().getSourceField()), Long.class);
		conn.hIncrBy(key, getConfig().getCommand().getTargetField(), delta);
	}

}