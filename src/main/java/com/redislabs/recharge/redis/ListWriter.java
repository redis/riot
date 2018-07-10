package com.redislabs.recharge.redis;

import java.util.Map;

import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.redislabs.recharge.config.KeyConfiguration;
import com.redislabs.recharge.config.ListConfiguration;

public class ListWriter extends AbstractCollectionWriter {

	public ListWriter(KeyConfiguration keyConfig, StringRedisTemplate template, ListConfiguration config) {
		super(keyConfig, template, config.getKey());
	}

	@Override
	protected void write(StringRedisConnection conn, String collectionKey, String id, Map<String, Object> map) {
		conn.lPush(collectionKey, id);
	}

}
