package com.redislabs.recharge.redisearch;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.springframework.batch.item.ExecutionContext;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.search.AddOptions;
import com.redislabs.lettusearch.search.DropOptions;
import com.redislabs.lettusearch.search.Schema;
import com.redislabs.recharge.IndexedPartitioner;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Setter
public class AddWriter extends AbstractRediSearchWriter {

	private boolean dropKeepDocs;
	private String[] keys;
	private String scoreField;
	private AddOptions options;
	private double defaultScore;
	private boolean drop;
	private Schema schema;

	@Override
	protected RedisFuture<?> write(Map<String, Object> record, RediSearchAsyncCommands<String, String> commands) {
		double score = getScore(record);
		String id = getValues(record, keys);
		return commands.add(index, id, score, toStringMap(record), options);
	}

	private double getScore(Map<String, Object> record) {
		return converter.convert(record.getOrDefault(scoreField, defaultScore), Double.class);
	}

	@Override
	public void open(ExecutionContext executionContext) {
		if (IndexedPartitioner.getPartitionIndex(executionContext) == 0) {
			try {
				StatefulRediSearchConnection<String, String> connection = pool.borrowObject();
				try {
					RediSearchAsyncCommands<String, String> commands = connection.async();
					if (drop) {
						log.info("Dropping index {}", index);
						try {
							RedisFuture<String> drop = commands.drop(index,
									DropOptions.builder().keepDocs(dropKeepDocs).build());
							commands.flushCommands();
							LettuceFutures.awaitOrCancel(drop, 10, TimeUnit.SECONDS);
						} catch (Exception e) {
							if (log.isDebugEnabled()) {
								log.debug("Could not drop index {}", index, e);
							} else {
								log.warn("Could not drop index {}", index);
							}
						}
					}
					if (schema != null) {
						log.debug("Creating schema {}", index);
						try {
							RedisFuture<String> create = commands.create(index, schema);
							commands.flushCommands();
							LettuceFutures.awaitOrCancel(create, 10, TimeUnit.SECONDS);
						} catch (Exception e) {
							if (e.getMessage().startsWith("Index already exists")) {
								log.debug("Ignored failure to create index {}", index, e);
							} else {
								log.error("Could not create index {}", index, e);
							}
						}
					}
				} finally {
					pool.returnObject(connection);
				}
			} catch (Exception e) {
				log.error("Could not create schema", e);
			}
		}
		super.open(executionContext);
	}

}
