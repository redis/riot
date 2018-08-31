package com.redislabs.recharge.redis.index;

import java.util.Map;

import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.redislabs.recharge.RechargeConfiguration.EntityConfiguration;
import com.redislabs.recharge.RechargeConfiguration.IndexConfiguration;
import com.redislabs.recharge.redis.AbstractRedisWriter;

public abstract class AbstractIndexWriter extends AbstractRedisWriter {

	private IndexConfiguration config;

	protected AbstractIndexWriter(StringRedisTemplate template, EntityConfiguration entity, IndexConfiguration index) {
		super(template, index.getName(), entity.getKeys());
		this.config = index;
	}

	protected IndexConfiguration getConfig() {
		return config;
	}

	@Override
	protected void write(StringRedisConnection conn, String keyspace, String id, Map<String, Object> record) {
		String indexKey = getIndexKey(keyspace, record);
		writeIndex(conn, indexKey, id, record);
	}

	private String getIndexKey(String keyspace, Map<String, Object> record) {
		if (config.getKeys() == null || config.getKeys().length == 0) {
			return keyspace;
		}
		String indexId = getValues(record, config.getKeys());
		return String.join(KEY_SEPARATOR, keyspace, indexId);
	}

	protected abstract void writeIndex(StringRedisConnection conn, String key, String id, Map<String, Object> record);

}
