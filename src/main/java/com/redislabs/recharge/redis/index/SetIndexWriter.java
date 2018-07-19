package com.redislabs.recharge.redis.index;

import java.util.Map;
import java.util.Map.Entry;

import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.redislabs.recharge.RechargeConfiguration.EntityConfiguration;
import com.redislabs.recharge.RechargeConfiguration.IndexConfiguration;

public class SetIndexWriter extends AbstractIndexWriter {

	public SetIndexWriter(StringRedisTemplate template, Entry<String, EntityConfiguration> entity,
			IndexConfiguration config) {
		super(template, entity, config);
	}

	@Override
	protected void write(StringRedisConnection conn, String key, Map<String, Object> entity, String id) {
		conn.sAdd(key, id);
	}

	@Override
	protected String getDefaultKeyspace() {
		return "set";
	}

}
