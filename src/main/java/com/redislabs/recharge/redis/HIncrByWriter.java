package com.redislabs.recharge.redis;

import java.util.Map;

import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.redislabs.recharge.RechargeConfiguration.HIncrByConfiguration;
import com.redislabs.recharge.RechargeConfiguration.RedisWriterConfiguration;

public class HIncrByWriter extends AbstractRedisWriter {

	private HIncrByConfiguration hincrby;

	public HIncrByWriter(StringRedisTemplate template, RedisWriterConfiguration entity) {
		super(template, entity);
		this.hincrby = entity.getHash().getIncrby();
	}

	@Override
	protected void write(StringRedisConnection conn, String key, Map<String, Object> fields) {
		Long delta = convert(fields.get(hincrby.getSourceField()), Long.class);
		conn.hIncrBy(key, hincrby.getTargetField(), delta);
	}

}