package com.redislabs.recharge.redis.index;

import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.redislabs.recharge.Entity;
import com.redislabs.recharge.RechargeConfiguration.EntityConfiguration;
import com.redislabs.recharge.RechargeConfiguration.IndexConfiguration;

public class ZSetIndexWriter extends AbstractIndexWriter {

	public ZSetIndexWriter(EntityConfiguration entityConfig, StringRedisTemplate template,
			IndexConfiguration indexConfig) {
		super(entityConfig, template, indexConfig);
	}

	@Override
	protected void write(StringRedisConnection conn, Entity entity, String id, String indexKey) {
		conn.zAdd(indexKey, getScore(entity), id);
	}

	private Double getScore(Entity entity) {
		return getValue(entity, getConfig().getScore(), Double.class);
	}

}
