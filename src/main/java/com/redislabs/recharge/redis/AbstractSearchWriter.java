package com.redislabs.recharge.redis;

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
import com.redislabs.recharge.RechargeConfiguration.FTCommandConfiguration;
import com.redislabs.recharge.RechargeConfiguration.RediSearchField;
import com.redislabs.recharge.RechargeConfiguration.RediSearchFieldType;
import com.redislabs.recharge.RechargeConfiguration.RedisWriterConfiguration;
import com.redislabs.recharge.RechargeConfiguration.SearchConfiguration;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractSearchWriter extends AbstractRedisWriter {

	private RediSearchClient client;
	private RediSearchCommands<String, String> commands;
	private SearchConfiguration search;

	public AbstractSearchWriter(StringRedisTemplate template, RedisWriterConfiguration writer,
			RediSearchClient rediSearchClient) {
		super(template, writer);
		this.search = writer.getSearch();
		this.client = rediSearchClient;
	}

	@Override
	public void open(ExecutionContext executionContext) {
		commands = client.connect().sync();
		if (!search.getSchema().isEmpty()) {
			SchemaBuilder builder = Schema.builder();
			search.getSchema().forEach(entry -> builder.field(getField(entry)));
			if (search.isDrop()) {
				try {
					commands.drop(search.getIndex());
				} catch (Exception e) {
					log.debug("Could not drop index {}", search.getIndex(), e);
				}
			}
			commands.create(search.getIndex(), builder.build());
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
	protected void write(StringRedisConnection conn, String key, Map<String, Object> record) {
		write(commands, search.getIndex(), key, record);
	}

	protected abstract void write(RediSearchCommands<String, String> commands, String index, String key,
			Map<String, Object> record);

	protected double getScore(FTCommandConfiguration search, Map<String, Object> record) {
		if (search.getScore() == null) {
			return search.getDefaultScore();
		}
		return convert(record.get(search.getScore()), Double.class);

	}

}
