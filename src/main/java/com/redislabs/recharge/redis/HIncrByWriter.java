package com.redislabs.recharge.redis;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.recharge.RechargeConfiguration.HIncrByConfiguration;
import com.redislabs.recharge.RechargeConfiguration.RedisWriterConfiguration;

public class HIncrByWriter extends AbstractRedisWriter {

	private HIncrByConfiguration hincrby;

	public HIncrByWriter(RediSearchClient client, RedisWriterConfiguration entity) {
		super(client, entity);
		this.hincrby = entity.getHash().getIncrby();
	}

	@Override
	protected void write(String key, Map<String, Object> fields) {
		Long delta = convert(fields.get(hincrby.getSourceField()), Long.class);
		commands.hincrby(key, hincrby.getTargetField(), delta);
	}

}