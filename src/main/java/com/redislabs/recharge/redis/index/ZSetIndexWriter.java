package com.redislabs.recharge.redis.index;

import java.util.Map;

import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.redislabs.recharge.RechargeConfiguration.EntityConfiguration;
import com.redislabs.recharge.RechargeConfiguration.IndexConfiguration;

public class ZSetIndexWriter extends AbstractIndexWriter {

	public ZSetIndexWriter(StringRedisTemplate template, EntityConfiguration entity, IndexConfiguration index) {
		super(template, entity, index);
	}

	@Override
	protected void writeIndex(StringRedisConnection conn, String key, String id, Map<String, Object> record) {
		Double score = getConfig().getScore() == null ? 1d : convert(record.get(getConfig().getScore()), Double.class);
		conn.zAdd(key, score, id);
	}

}
