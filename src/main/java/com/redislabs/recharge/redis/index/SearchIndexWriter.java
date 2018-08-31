package com.redislabs.recharge.redis.index;

import java.util.Map;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.RediSearchCommands;
import com.redislabs.lettusearch.index.Field;
import com.redislabs.lettusearch.index.GeoField;
import com.redislabs.lettusearch.index.NumericField;
import com.redislabs.lettusearch.index.Schema;
import com.redislabs.lettusearch.index.Schema.SchemaBuilder;
import com.redislabs.lettusearch.index.TextField;
import com.redislabs.recharge.RechargeConfiguration.EntityConfiguration;
import com.redislabs.recharge.RechargeConfiguration.IndexConfiguration;
import com.redislabs.recharge.RechargeConfiguration.RediSearchField;
import com.redislabs.recharge.RechargeConfiguration.RediSearchFieldType;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SearchIndexWriter extends AbstractIndexWriter {

	private RediSearchClient client;
	private RediSearchCommands<String, String> commands;

	public SearchIndexWriter(StringRedisTemplate template, EntityConfiguration entity, IndexConfiguration index,
			RediSearchClient client) {
		super(template, entity, index);
		this.client = client;
	}

	@Override
	public void open(ExecutionContext executionContext) {
		commands = client.connect().sync();
		IndexConfiguration index = getConfig();
		if (!index.getSchemaFields().isEmpty()) {
			SchemaBuilder builder = Schema.builder();
			index.getSchemaFields().forEach(entry -> builder.field(getField(entry)));
			if (index.isDrop()) {
				try {
					commands.drop(index.getName());
				} catch (Exception e) {
					log.debug("Could not drop index {}", index.getName(), e);
				}
			}
			commands.create(index.getName(), builder.build());
		}
	}

	private Field getField(RediSearchField fieldConfig) {
		Field field = createField(fieldConfig.getName(), fieldConfig.getType());
		field.setNoIndex(fieldConfig.isNoIndex());
		field.setSortable(fieldConfig.isSortable());
		return field;
	}

	private Field createField(String name, RediSearchFieldType type) {
		switch (type) {
		case Geo:
			return new GeoField(name);
		case Numeric:
			return new NumericField(name);
		default:
			return new TextField(name);
		}
	}

	@Override
	protected void writeIndex(StringRedisConnection conn, String key, String id, Map<String, Object> record) {
		try {
			commands.addHash(getConfig().getName(), key, 1);
		} catch (Exception e) {
			if ("Document already exists".equals(e.getMessage())) {
				log.debug(e.getMessage());
			} else {
				log.error("Could not add document: {}", e.getMessage());
			}
		}
	}

}
