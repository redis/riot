package com.redislabs.recharge.redis.index;

import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.redislabs.recharge.Entity;
import com.redislabs.recharge.RechargeConfiguration.EntityConfiguration;
import com.redislabs.recharge.RechargeConfiguration.IndexConfiguration;

public class SetIndexWriter extends AbstractIndexWriter {

	public SetIndexWriter(EntityConfiguration entityConfig, StringRedisTemplate template,
			IndexConfiguration indexConfig) {
		super(entityConfig, template, indexConfig);
	}

	@Override
	protected void write(StringRedisConnection conn, Entity entity, String id, String indexKey) {
		conn.sAdd(indexKey, id);
	}

}
