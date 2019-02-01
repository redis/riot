package com.redislabs.recharge.writer.redis;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.recharge.RechargeConfiguration.RedisWriterConfiguration;
import com.redislabs.recharge.RechargeConfiguration.ZSetConfiguration;

public class ZAddWriter extends AbstractCollectionRedisWriter {

	private ZSetConfiguration zset;

	public ZAddWriter(RediSearchClient client, RedisWriterConfiguration writer) {
		super(client, writer, writer.getZset());
		this.zset = writer.getZset();
	}

	@Override
	protected void write(String key, Map<String, Object> record) {
		Double score = zset.getScore() == null ? zset.getDefaultScore()
				: convert(record.get(zset.getScore()), Double.class);
		commands.zadd(key, score, getMemberId(record));
	}

}
