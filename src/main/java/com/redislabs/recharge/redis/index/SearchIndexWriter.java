package com.redislabs.recharge.redis.index;

import java.util.Map;
import java.util.Map.Entry;

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

	private Entry<String, IndexConfiguration> index;
	private RediSearchClient client;
	private RediSearchCommands<String, String> commands;
	private String indexName;

	public SearchIndexWriter(StringRedisTemplate template, Entry<String, EntityConfiguration> entity,
			Entry<String, IndexConfiguration> index, RediSearchClient client) {
		super(template, entity, index);
		this.index = index;
		this.indexName = index.getKey();
		this.client = client;
	}

	@Override
	public void open(ExecutionContext executionContext) {
		commands = client.connect().sync();
		IndexConfiguration indexConfig = index.getValue();
		if (!indexConfig.getSchema().isEmpty()) {
			SchemaBuilder builder = Schema.builder();
			indexConfig.getSchema().entrySet().forEach(entry -> builder.field(getField(entry)));
			if (indexConfig.isDrop()) {
				try {
					commands.drop(indexName);
				} catch (Exception e) {
					log.debug("Could not drop index {}", indexName, e);
				}
			}
			commands.create(indexName, builder.build());
		}
	}

	private Field getField(Entry<String, RediSearchField> entry) {
		RediSearchField fieldConfig = entry.getValue();
		Field field = createField(entry.getKey(), fieldConfig.getType());
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
	protected void write(StringRedisConnection conn, Map<String, Object> record, String id, String key,
			String indexKey) {
		try {
			commands.addHash(indexName, key, 1);
		} catch (Exception e) {
			if ("Document already exists".equals(e.getMessage())) {
				log.debug(e.getMessage());
			} else {
				log.error("Could not add document: {}", e.getMessage());
			}
		}
	}

	@Override
	protected String getDefaultKeyspace() {
		return "search";
	}

}
