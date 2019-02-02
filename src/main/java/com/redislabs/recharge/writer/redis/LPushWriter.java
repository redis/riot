package com.redislabs.recharge.writer.redis;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.recharge.RechargeConfiguration.RedisWriterConfiguration;

@SuppressWarnings("rawtypes")
public class LPushWriter extends AbstractCollectionRedisWriter {

	public LPushWriter(RediSearchClient client, RedisWriterConfiguration writer) {
		super(client, writer, writer.getList());
	}

	@Override
	protected void write(String key, Map record) {
		commands.lpush(key, getMemberId(record));
	}

}
