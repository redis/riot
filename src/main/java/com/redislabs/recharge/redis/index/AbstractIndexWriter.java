package com.redislabs.recharge.redis.index;

import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.redislabs.recharge.Entity;
import com.redislabs.recharge.RechargeConfiguration.EntityConfiguration;
import com.redislabs.recharge.RechargeConfiguration.IndexConfiguration;
import com.redislabs.recharge.redis.AbstractTemplateWriter;

public abstract class AbstractIndexWriter extends AbstractTemplateWriter {

	private IndexConfiguration config;

	protected AbstractIndexWriter(EntityConfiguration entityConfig, StringRedisTemplate template,
			IndexConfiguration config) {
		super(entityConfig, template);
		this.config = config;
	}

	public IndexConfiguration getConfig() {
		return config;
	}

	@Override
	protected void write(StringRedisConnection conn, Entity entity, String id) {
		String indexKey = entity.getName() + AbstractTemplateWriter.KEY_SEPARATOR + config.getField()
				+ AbstractTemplateWriter.KEY_SEPARATOR + getIndexId(entity);
		write(conn, entity, id, indexKey);
	}

	protected abstract void write(StringRedisConnection conn, Entity entity, String id, String indexKey);

	private String getIndexId(Entity entity) {
		return getValue(entity, config.getField(), String.class);
	}

}
