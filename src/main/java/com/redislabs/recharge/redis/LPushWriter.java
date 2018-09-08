package com.redislabs.recharge.redis;

import java.util.Map;

import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.redislabs.recharge.RechargeConfiguration.RedisWriterConfiguration;

public class LPushWriter extends AbstractCollectionRedisWriter {

	public LPushWriter(StringRedisTemplate template, RedisWriterConfiguration writer) {
		super(template, writer, writer.getList());
	}
	
	@Override
	protected void write(StringRedisConnection conn, String key, Map<String, Object> record) {
		conn.lPush(key, getMemberId(record));
	}

}
