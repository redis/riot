package com.redislabs.recharge.redis;

import java.util.Map;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.redislabs.recharge.RechargeConfiguration.RediSearchField;
import com.redislabs.recharge.RechargeConfiguration.RediSearchFieldType;
import com.redislabs.recharge.RechargeConfiguration.RedisWriterConfiguration;
import com.redislabs.recharge.RechargeConfiguration.SearchConfiguration;

import io.redisearch.Schema;
import io.redisearch.Schema.Field;
import io.redisearch.Schema.FieldType;
import io.redisearch.client.Client;
import io.redisearch.client.Client.IndexOptions;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractSearchWriter extends AbstractRedisWriter {

	private Client client;

	public AbstractSearchWriter(StringRedisTemplate template, RedisWriterConfiguration writer, Client client) {
		super(template, writer);
		this.client = client;
	}

	@Override
	public void open(ExecutionContext executionContext) {
		SearchConfiguration search = getConfig().getSearch();
		if (!search.getSchema().isEmpty()) {
			Schema schema = new Schema();
			search.getSchema().forEach(entry -> schema.addField(getField(entry)));
			if (search.isDrop()) {
				try {
					client.dropIndex();
				} catch (Exception e) {
					log.debug("Could not drop index {}", getConfig().getKeyspace(), e);
				}
			}
			if (search.isCreate()) {
				try {
					client.createIndex(schema, IndexOptions.Default());
				} catch (Exception e) {
					if (e.getMessage().startsWith("Index already exists")) {
						log.debug("Ignored index {} creation fail", getConfig().getKeyspace(), e);
					} else {
						log.error("Could not create index {}", getConfig().getKeyspace(), e);
					}
				}
			}
		}
	}

	@Override
	public void close() {
		client.close();
		super.close();
	}

	private Field getField(RediSearchField fieldConfig) {
		return new Field(fieldConfig.getName(), getFieldType(fieldConfig.getType()), fieldConfig.isSortable(),
				fieldConfig.isNoIndex());
	}

	private FieldType getFieldType(RediSearchFieldType type) {
		switch (type) {
		case Geo:
			return FieldType.Geo;
		case Numeric:
			return FieldType.Numeric;
		default:
			return FieldType.FullText;
		}
	}

	@Override
	protected void write(StringRedisConnection conn, String key, Map<String, Object> record) {
		write(client, key, record);
	}

	protected abstract void write(Client client, String key, Map<String, Object> record);

	protected double getScore(Map<String, Object> record) {
		if (getConfig().getSearch().getScore() == null) {
			return getConfig().getSearch().getDefaultScore();
		}
		return convert(record.get(getConfig().getSearch().getScore()), Double.class);

	}

}
