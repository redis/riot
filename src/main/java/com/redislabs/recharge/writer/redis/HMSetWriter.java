package com.redislabs.recharge.writer.redis;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.recharge.RechargeConfiguration.RedisWriterConfiguration;

public class HMSetWriter extends AbstractPipelineRedisWriter {

	public HMSetWriter(RediSearchClient client, RedisWriterConfiguration entity) {
		super(client, entity);
	}

	@Override
	protected void write(String key, Map<String, Object> record) {
		commands.hmset(key, convert(record));
	}

}