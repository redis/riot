package com.redislabs.recharge.redis;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.recharge.RechargeConfiguration.RedisWriterConfiguration;

public class LPushWriter extends AbstractCollectionRedisWriter {

	public LPushWriter(RediSearchClient client, RedisWriterConfiguration writer) {
		super(client, writer, writer.getList());
	}

	@Override
	protected void write(String key, Map<String, Object> record) {
		commands.lpush(key, getMemberId(record));
	}

}
