package com.redislabs.recharge.redis;

import java.util.Map;

import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.redislabs.recharge.RechargeConfiguration.RedisWriterConfiguration;

public class SAddWriter extends AbstractCollectionRedisWriter {

	public SAddWriter(StringRedisTemplate template, RedisWriterConfiguration writer) {
		super(template, writer, writer.getSet());
	}

	@Override
	protected void write(StringRedisConnection conn, String key, Map<String, Object> record) {
		conn.sAdd(key, getMemberId(record));
	}

}
