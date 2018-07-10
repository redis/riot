package com.redislabs.recharge.redis;

import java.util.Map;

import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.redislabs.recharge.config.KeyConfiguration;
import com.redislabs.recharge.config.SetConfiguration;

public class SetWriter extends AbstractCollectionWriter {

	public SetWriter(KeyConfiguration keyConfig, StringRedisTemplate template, SetConfiguration config) {
		super(keyConfig, template, config.getKey());
	}

	@Override
	protected void write(StringRedisConnection conn, String collectionKey, String id, Map<String, Object> map) {
		conn.sAdd(collectionKey, id);
	}

}
