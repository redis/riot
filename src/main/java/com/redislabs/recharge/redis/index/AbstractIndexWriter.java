package com.redislabs.recharge.redis.index;

import java.util.Map;
import java.util.Map.Entry;

import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.redislabs.recharge.RechargeConfiguration.EntityConfiguration;
import com.redislabs.recharge.RechargeConfiguration.IndexConfiguration;
import com.redislabs.recharge.redis.AbstractEntityWriter;
import com.redislabs.recharge.redis.key.AbstractKeyBuilder;
import com.redislabs.recharge.redis.key.KeyBuilder;
import com.redislabs.recharge.redis.key.MultipleFieldKeyBuilder;

public abstract class AbstractIndexWriter extends AbstractEntityWriter {

	private KeyBuilder indexKeyBuilder;

	protected AbstractIndexWriter(StringRedisTemplate template, Entry<String, EntityConfiguration> entity,
			IndexConfiguration indexConfig) {
		super(template, entity);
		indexKeyBuilder = AbstractKeyBuilder.getKeyBuilder(getKeyspace(entity, indexConfig), indexConfig.getFields());
	}

	private String getKeyspace(Entry<String, EntityConfiguration> entity, IndexConfiguration indexConfig) {
		return MultipleFieldKeyBuilder.join(entity.getKey(), getKeyspace(indexConfig));
	}

	private String getKeyspace(IndexConfiguration indexConfig) {
		if (indexConfig.getName() == null) {
			if (indexConfig.getFields() == null || indexConfig.getFields().length == 0) {
				return getDefaultKeyspace();
			}
			return MultipleFieldKeyBuilder.join(indexConfig.getFields());
		}
		return indexConfig.getName();
	}

	protected abstract String getDefaultKeyspace();

	@Override
	protected String getKey(Map<String, Object> record) {
		return indexKeyBuilder.getKey(record);
	}

	@Override
	protected void write(StringRedisConnection conn, String key, Map<String, Object> record) {
		write(conn, key, record, getId(record));
	}

	protected abstract void write(StringRedisConnection conn, String key, Map<String, Object> record, String id);

}
