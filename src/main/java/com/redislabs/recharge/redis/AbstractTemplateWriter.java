
package com.redislabs.recharge.redis;

import java.util.List;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.redislabs.recharge.Entity;
import com.redislabs.recharge.RechargeConfiguration.EntityConfiguration;

public abstract class AbstractTemplateWriter extends AbstractItemStreamItemWriter<Entity> {

	public static final String KEY_SEPARATOR = ":";
	private StringRedisTemplate template;
	private ConversionService conversionService = new DefaultConversionService();
	private List<String> keys;

	protected AbstractTemplateWriter(EntityConfiguration config, StringRedisTemplate template) {
		this.keys = config.getKeys();
		this.template = template;
	}

	protected <T> T getValue(Entity entity, String field, Class<T> targetType) {
		return conversionService.convert(entity.get(field), targetType);
	}

	protected String getString(Entity entity, String field) {
		return getValue(entity, field, String.class);
	}

	@Override
	public void open(ExecutionContext executionContext) {
		super.open(executionContext);
	}

	@Override
	public void write(List<? extends Entity> items) {
		template.executePipelined(new RedisCallback<Object>() {
			public Object doInRedis(RedisConnection connection) throws DataAccessException {
				StringRedisConnection conn = (StringRedisConnection) connection;
				for (Entity entity : items) {
					write(conn, entity, getId(entity));
				}
				return null;
			}
		});
	}

	protected abstract void write(StringRedisConnection conn, Entity entity, String id);

	protected String getKey(Entity entity, String id) {
		return entity.getName() + KEY_SEPARATOR + id;
	}

	protected String getId(Entity entity) {
		String id = getString(entity, keys.get(0));
		for (String key : keys.subList(1, keys.size())) {
			id += KEY_SEPARATOR + getString(entity, key);
		}
		return id;
	}

}
