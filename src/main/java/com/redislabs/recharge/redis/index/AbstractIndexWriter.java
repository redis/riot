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
	protected String keyspace;

	protected AbstractIndexWriter(StringRedisTemplate template, Entry<String, EntityConfiguration> entity,
			Entry<String, IndexConfiguration> index) {
		super(template, entity);
		this.keyspace = MultipleFieldKeyBuilder.join(entity.getKey(), getKeyspace(index));
		this.indexKeyBuilder = AbstractKeyBuilder.getKeyBuilder(index.getValue().getFields());
	}

	private String getKeyspace(Entry<String, IndexConfiguration> index) {
		if (index.getKey() == null) {
			if (index.getValue().getFields() == null || index.getValue().getFields().length == 0) {
				return getDefaultKeyspace();
			}
			return MultipleFieldKeyBuilder.join(index.getValue().getFields());
		}
		return index.getKey();
	}

	protected abstract String getDefaultKeyspace();

	@Override
	protected void write(StringRedisConnection conn, Map<String, Object> record, String id, String key) {
		String indexId = indexKeyBuilder.getId(record);
		String indexKey = keyspace + KeyBuilder.KEY_SEPARATOR + indexId;
		write(conn, record, id, key, indexKey);
	}

	protected void write(StringRedisConnection conn, Map<String, Object> record, String id, String key,
			String indexKey) {
		// do nothing
	}

}
