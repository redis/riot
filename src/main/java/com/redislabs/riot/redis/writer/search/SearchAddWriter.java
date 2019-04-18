package com.redislabs.riot.redis.writer.search;

import java.util.HashMap;
import java.util.Map;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.core.convert.support.DefaultConversionService;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.search.AddOptions;
import com.redislabs.lettusearch.search.Schema;
import com.redislabs.riot.redis.RedisConverter;

import io.lettuce.core.RedisFuture;
import lombok.Setter;

@Setter
public class SearchAddWriter extends AbstractRediSearchWriter {

	private boolean dropKeepDocs;
	private String[] keys;
	private String scoreField;
	private String payloadField;
	private AddOptions options;
	private double defaultScore;
	private boolean drop;
	private Schema schema;
	private RedisConverter redisConverter;
	private DefaultConversionService converter;

	//TODO use AbstractRedisCommands
	private Map<String, String> stringMap(Map<String, Object> item) {
		Map<String, String> stringMap = new HashMap<String, String>();
		for (String key : item.keySet()) {
			Object value = item.get(key);
			stringMap.put(key, converter.convert(value, String.class));
		}
		return stringMap;
	}

	protected RedisFuture<?> write(Map<String, Object> record, RediSearchAsyncCommands<String, String> commands) {
		double score = getScore(record);
		String id = redisConverter.joinFields(record, keys);
		String payload = payload(record);
		return commands.add(index, id, score, stringMap(record), options, payload);
	}

	private String payload(Map<String, Object> record) {
		if (payloadField == null) {
			return null;
		}
		return converter.convert(record.get(payloadField), String.class);
	}

	private double getScore(Map<String, Object> record) {
		return converter.convert(record.getOrDefault(scoreField, defaultScore), Double.class);
	}

	public void open(ExecutionContext executionContext) {
//		if (IndexedPartitioner.getPartitionIndex(executionContext) == 0) {
//			try {
//				StatefulRediSearchConnection<String, String> connection = pool.borrowObject();
//				try {
//					RediSearchAsyncCommands<String, String> commands = connection.async();
//					if (drop) {
//						log.info("Dropping index {}", index);
//						try {
//							RedisFuture<String> drop = commands.drop(index,
//									DropOptions.builder().keepDocs(dropKeepDocs).build());
//							commands.flushCommands();
//							LettuceFutures.awaitOrCancel(drop, 10, TimeUnit.SECONDS);
//						} catch (Exception e) {
//							if (log.isDebugEnabled()) {
//								log.debug("Could not drop index {}", index, e);
//							} else {
//								log.warn("Could not drop index {}", index);
//							}
//						}
//					}
//					if (schema != null) {
//						log.debug("Creating schema {}", index);
//						try {
//							RedisFuture<String> create = commands.create(index, schema);
//							commands.flushCommands();
//							LettuceFutures.awaitOrCancel(create, 10, TimeUnit.SECONDS);
//						} catch (Exception e) {
//							if (e.getMessage().startsWith("Index already exists")) {
//								log.debug("Ignored failure to create index {}", index, e);
//							} else {
//								log.error("Could not create index {}", index, e);
//							}
//						}
//					}
//				} finally {
//					pool.returnObject(connection);
//				}
//			} catch (Exception e) {
//				log.error("Could not create schema", e);
//			}
//		}
	}

}
