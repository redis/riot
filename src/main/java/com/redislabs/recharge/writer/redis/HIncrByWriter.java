package com.redislabs.recharge.writer.redis;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.recharge.RechargeConfiguration.HIncrByConfiguration;
import com.redislabs.recharge.RechargeConfiguration.RedisWriterConfiguration;

@SuppressWarnings("rawtypes")
public class HIncrByWriter extends AbstractPipelineRedisWriter {

	private HIncrByConfiguration hincrby;

	public HIncrByWriter(RediSearchClient client, RedisWriterConfiguration entity) {
		super(client, entity);
		this.hincrby = entity.getHash().getIncrby();
	}

	@Override
	protected void write(String key, Map fields) {
		Long delta = convert(fields.get(hincrby.getSourceField()), Long.class);
		commands.hincrby(key, hincrby.getTargetField(), delta);
	}

}