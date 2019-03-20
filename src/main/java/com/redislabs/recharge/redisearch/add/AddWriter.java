package com.redislabs.recharge.redisearch.add;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.springframework.batch.item.ExecutionContext;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.search.AddOptions;
import com.redislabs.lettusearch.search.DropOptions;
import com.redislabs.lettusearch.search.Schema;
import com.redislabs.lettusearch.search.Schema.SchemaBuilder;
import com.redislabs.lettusearch.search.field.Field;
import com.redislabs.lettusearch.search.field.GeoField;
import com.redislabs.lettusearch.search.field.NumericField;
import com.redislabs.lettusearch.search.field.TagField;
import com.redislabs.lettusearch.search.field.TextField;
import com.redislabs.recharge.IndexedPartitioner;
import com.redislabs.recharge.redisearch.RediSearchCommandWriter;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import lombok.extern.slf4j.Slf4j;

@SuppressWarnings({ "rawtypes", "unchecked" })
@Slf4j
public class AddWriter extends RediSearchCommandWriter<AddConfiguration> {

	private AddOptions options;

	public AddWriter(AddConfiguration config) {
		super(config);
		options = AddOptions.builder().noSave(config.isNoSave()).replace(config.isReplace())
				.replacePartial(config.isReplacePartial()).build();
	}

	@Override
	protected RedisFuture<?> write(Map record, RediSearchAsyncCommands<String, String> commands) {
		double score = getScore(record);
		String id = getValues(record, config.getKeys());
		convert(record);
		return commands.add(config.getIndex(), id, score, record, options);
	}

	private double getScore(Map<String, Object> record) {
		if (config.getScore() == null) {
			return config.getDefaultScore();
		}
		return converter.convert(record.get(config.getScore()), Double.class);
	}

	@Override
	public void open(ExecutionContext executionContext) {
		if (IndexedPartitioner.getPartitionIndex(executionContext) != 0) {
			return;
		}
		try {
			StatefulRediSearchConnection<String, String> connection = pool.borrowObject();
			try {
				RediSearchAsyncCommands<String, String> commands = connection.async();
				if (config.isDrop()) {
					log.info("Dropping index {}", config.getIndex());
					try {
						RedisFuture<String> drop = commands.drop(config.getIndex(),
								DropOptions.builder().keepDocs(config.isDropKeepDocs()).build());
						commands.flushCommands();
						LettuceFutures.awaitOrCancel(drop, 10, TimeUnit.SECONDS);
					} catch (Exception e) {
						if (log.isDebugEnabled()) {
							log.debug("Could not drop index {}", config.getIndex(), e);
						} else {
							log.warn("Could not drop index {}", config.getIndex());
						}
					}
				}
				if (config.isCreate() && !config.getSchema().isEmpty()) {
					SchemaBuilder builder = Schema.builder();
					config.getSchema().forEach(field -> builder.field(field(field)));
					Schema schema = builder.build();
					log.debug("Creating schema {}", config.getIndex());
					try {
						RedisFuture<String> create = commands.create(config.getIndex(), schema);
						commands.flushCommands();
						LettuceFutures.awaitOrCancel(create, 10, TimeUnit.SECONDS);
					} catch (Exception e) {
						if (e.getMessage().startsWith("Index already exists")) {
							log.debug("Ignored failure to create index {}", config.getIndex(), e);
						} else {
							log.error("Could not create index {}", config.getIndex(), e);
						}
					}
				}
			} finally {
				pool.returnObject(connection);
			}
		} catch (Exception e) {
			log.error("Could not create schema", e);
		}
		super.open(executionContext);
	}

	private Field field(SchemaField field) {
		switch (field.getType()) {
		case Geo:
			return GeoField.builder().name(field.getName()).sortable(field.isSortable()).noIndex(field.isNoIndex())
					.build();
		case Numeric:
			return NumericField.builder().name(field.getName()).sortable(field.isSortable()).noIndex(field.isNoIndex())
					.build();
		case Tag:
			return TagField.builder().name(field.getName()).sortable(field.isSortable()).noIndex(field.isNoIndex())
					.build();
		default:
			return TextField.builder().name(field.getName()).sortable(field.isSortable()).noIndex(field.isNoIndex())
					.matcher(field.getMatcher()).noStem(field.isNoStem()).weight(field.getWeight()).build();
		}
	}

}
