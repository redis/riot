package com.redislabs.recharge.redis.writers;

import java.util.Map;

import org.springframework.batch.item.ExecutionContext;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.search.AddOptions;
import com.redislabs.lettusearch.search.DropOptions;
import com.redislabs.lettusearch.search.Schema;
import com.redislabs.lettusearch.search.Schema.SchemaBuilder;
import com.redislabs.lettusearch.search.field.Field;
import com.redislabs.lettusearch.search.field.GeoField;
import com.redislabs.lettusearch.search.field.NumericField;
import com.redislabs.lettusearch.search.field.TextField;
import com.redislabs.lettusearch.search.field.TextField.TextFieldBuilder;
import com.redislabs.recharge.RechargeConfiguration.RediSearchField;
import com.redislabs.recharge.RechargeConfiguration.SearchConfiguration;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@SuppressWarnings({ "rawtypes", "unchecked" })
public class FTAddWriter extends AbstractPipelineRedisWriter<SearchConfiguration> {

	public FTAddWriter(SearchConfiguration config) {
		super(config);
	}

	@Override
	public void open(ExecutionContext executionContext) {
		super.open(executionContext);
		RediSearchAsyncCommands<String, String> commands = connection.async();
		String keyspace = config.getKeyspace();
		if (config.isDrop()) {
			log.debug("Dropping index {}", keyspace);
			try {
				commands.drop(keyspace, DropOptions.builder().build());
			} catch (Exception e) {
				log.debug("Could not drop index {}", keyspace, e);
			}
		}
		if (config.isCreate() && !config.getSchema().isEmpty()) {
			SchemaBuilder builder = Schema.builder();
			config.getSchema().forEach(entry -> builder.field(getField(entry)));
			Schema schema = builder.build();
			log.debug("Creating schema {}", keyspace);
			try {
				commands.create(keyspace, schema);
			} catch (Exception e) {
				if (e.getMessage().startsWith("Index already exists")) {
					log.debug("Ignored failure to create index {}", keyspace, e);
				} else {
					log.error("Could not create index {}", keyspace, e);
				}
			}
		}
		commands.flushCommands();
	}

	@Override
	protected void write(String key, Map record, RediSearchAsyncCommands<String, String> commands) {
		double score = getScore(record);
		AddOptions options = AddOptions.builder().noSave(config.isNoSave()).replace(config.isReplace())
				.replacePartial(config.isReplacePartial()).build();
		try {
			commands.add(config.getKeyspace(), key, score, convert(record), options);
		} catch (Exception e) {
			if ("Document already exists".equals(e.getMessage())) {
				log.debug(e.getMessage());
			} else {
				log.error("Could not add document: {}", e.getMessage());
			}
		}
	}

	private double getScore(Map<String, Object> record) {
		if (config.getScore() == null) {
			return config.getDefaultScore();
		}
		return converter.convert(record.get(config.getScore()), Double.class);
	}

	private Field getField(RediSearchField field) {
		switch (field.getType()) {
		case Geo:
			return GeoField.builder().name(field.getName()).sortable(field.isSortable()).build();
		case Numeric:
			return NumericField.builder().name(field.getName()).sortable(field.isSortable()).build();
		default:
			TextFieldBuilder builder = TextField.builder().name(field.getName()).sortable(field.isSortable())
					.noStem(field.isNoStem());
			if (field.getWeight() != null) {
				builder.weight(field.getWeight());
			}
			if (field.getMatcher() != null) {
				builder.matcher(field.getMatcher());
			}
			return builder.build();
		}
	}

}
