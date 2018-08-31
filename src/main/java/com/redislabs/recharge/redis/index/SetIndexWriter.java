package com.redislabs.recharge.redis.index;

import java.util.Map;

import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.redislabs.recharge.RechargeConfiguration.EntityConfiguration;
import com.redislabs.recharge.RechargeConfiguration.IndexConfiguration;

public class SetIndexWriter extends AbstractIndexWriter {

	public SetIndexWriter(StringRedisTemplate template, EntityConfiguration entity, IndexConfiguration index) {
		super(template, entity, index);
	}

	@Override
	protected void writeIndex(StringRedisConnection conn, String key, String id, Map<String, Object> record) {
		conn.sAdd(key, id);
	}

}
