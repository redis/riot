package com.redislabs.recharge.redis;

import java.util.Map;

import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.redislabs.recharge.RechargeConfiguration.KeyConfiguration;

public abstract class AbstractCollectionWriter extends AbstractTemplateWriter {

	private KeyBuilder collectionKeyBuilder;

	protected AbstractCollectionWriter(KeyConfiguration keyConfig, StringRedisTemplate template,
			KeyConfiguration collectionKeyConfig) {
		super(keyConfig, template);
		this.collectionKeyBuilder = new KeyBuilder(collectionKeyConfig);
	}

	@Override
	protected void write(StringRedisConnection conn, Map<String, Object> map) {
		String collectionKey = collectionKeyBuilder.getKey(map);
		write(conn, collectionKey, getId(map), map);
	}

	protected abstract void write(StringRedisConnection conn, String key, String id, Map<String, Object> map);

}
