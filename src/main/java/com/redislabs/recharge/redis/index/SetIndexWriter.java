package com.redislabs.recharge.redis.index;

import java.util.Map;
import java.util.Map.Entry;

import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.redislabs.recharge.RechargeConfiguration.EntityConfiguration;
import com.redislabs.recharge.RechargeConfiguration.IndexConfiguration;

public class SetIndexWriter extends AbstractIndexWriter {

	public SetIndexWriter(StringRedisTemplate template, Entry<String, EntityConfiguration> entity,
			Entry<String, IndexConfiguration> index) {
		super(template, entity, index);
	}
	
	@Override
	protected void write(StringRedisConnection conn, Map<String, Object> record, String id, String key,
			String indexKey) {
		conn.sAdd(indexKey, id);
	}

	@Override
	protected String getDefaultKeyspace() {
		return "set";
	}

}
