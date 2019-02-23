package com.redislabs.recharge.redis.search;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.pool2.impl.GenericObjectPool;
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
import com.redislabs.recharge.redis.PipelineRedisWriter;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import lombok.extern.slf4j.Slf4j;

@SuppressWarnings({ "rawtypes", "unchecked" })
@Slf4j
public class FTAddWriter extends PipelineRedisWriter<SearchConfiguration> {

	private AddOptions options;

	public FTAddWriter(SearchConfiguration config,
			GenericObjectPool<StatefulRediSearchConnection<String, String>> pool) {
		super(config, pool);
		options = AddOptions.builder().noSave(config.isNoSave()).replace(config.isReplace())
				.replacePartial(config.isReplacePartial()).build();
	}

	@Override
	protected RedisFuture<?> write(String id, Map record, RediSearchAsyncCommands<String, String> commands) {
		double score = getScore(record);
		convert(record);
		return commands.add(config.getKeyspace(), id, score, record, options);
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
				String keyspace = config.getKeyspace();
				if (config.isDrop()) {
					log.info("Dropping index {}", keyspace);
					try {
						RedisFuture<String> drop = commands.drop(keyspace,
								DropOptions.builder().keepDocs(config.isDropKeepDocs()).build());
						commands.flushCommands();
						LettuceFutures.awaitOrCancel(drop, 10, TimeUnit.SECONDS);
					} catch (Exception e) {
						if (log.isDebugEnabled()) {
							log.debug("Could not drop index {}", keyspace, e);
						} else {
							log.warn("Could not drop index {}", keyspace);
						}
					}
				}
				if (config.isCreate() && !config.getSchema().isEmpty()) {
					SchemaBuilder builder = Schema.builder();
					config.getSchema().forEach(entry -> builder.field(getField(entry)));
					Schema schema = builder.build();
					log.debug("Creating schema {}", keyspace);
					try {
						RedisFuture<String> create = commands.create(keyspace, schema);
						commands.flushCommands();
						LettuceFutures.awaitOrCancel(create, 10, TimeUnit.SECONDS);
					} catch (Exception e) {
						if (e.getMessage().startsWith("Index already exists")) {
							log.debug("Ignored failure to create index {}", keyspace, e);
						} else {
							log.error("Could not create index {}", keyspace, e);
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

	private Field getField(SearchField field) {
		if (field.getGeo() != null) {
			return GeoField.builder().name(field.getGeo().getName()).sortable(field.getGeo().isSortable())
					.noIndex(field.getGeo().isNoIndex()).build();
		}
		if (field.getNumeric() != null) {
			return NumericField.builder().name(field.getNumeric().getName()).sortable(field.getNumeric().isSortable())
					.noIndex(field.getNumeric().isNoIndex()).build();
		}
		if (field.getTag() != null) {
			return TagField.builder().name(field.getTag().getName()).sortable(field.getTag().isSortable())
					.noIndex(field.getTag().isNoIndex()).build();
		}
		return TextField.builder().name(field.getText().getName()).sortable(field.getText().isSortable())
				.noIndex(field.getText().isNoIndex()).matcher(field.getText().getMatcher())
				.noStem(field.getText().isNoStem()).weight(field.getText().getWeight()).build();
	}

}
