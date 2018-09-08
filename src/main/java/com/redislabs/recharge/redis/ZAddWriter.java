package com.redislabs.recharge.redis;

import java.util.Map;

import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.redislabs.recharge.RechargeConfiguration.RedisWriterConfiguration;
import com.redislabs.recharge.RechargeConfiguration.ZSetConfiguration;

public class ZAddWriter extends AbstractCollectionRedisWriter {

	private ZSetConfiguration zset;

	public ZAddWriter(StringRedisTemplate template, RedisWriterConfiguration writer) {
		super(template, writer, writer.getZset());
		this.zset = writer.getZset();
	}

	@Override
	protected void write(StringRedisConnection conn, String key, Map<String, Object> record) {
		Double score = zset.getScore() == null ? zset.getDefaultScore()
				: convert(record.get(zset.getScore()), Double.class);
		conn.zAdd(key, score, getMemberId(record));
	}

}
