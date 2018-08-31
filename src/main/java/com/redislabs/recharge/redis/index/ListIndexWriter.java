package com.redislabs.recharge.redis.index;

import java.util.Map;

import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.redislabs.recharge.RechargeConfiguration.EntityConfiguration;
import com.redislabs.recharge.RechargeConfiguration.IndexConfiguration;

public class ListIndexWriter extends AbstractIndexWriter {

	public ListIndexWriter(StringRedisTemplate template, EntityConfiguration entity, IndexConfiguration index) {
		super(template, entity, index);
	}

	@Override
	protected void writeIndex(StringRedisConnection conn, String indexKey, String id, Map<String, Object> record) {
		conn.lPush(indexKey, id);
	}

}
