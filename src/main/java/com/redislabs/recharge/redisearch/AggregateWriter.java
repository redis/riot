package com.redislabs.recharge.redisearch;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.aggregate.AggregateOptions;
import com.redislabs.recharge.MapTemplate;

import io.lettuce.core.RedisFuture;
import lombok.AccessLevel;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Setter
public class AggregateWriter extends AbstractRediSearchWriter {

	private String query;
	private AggregateOptions options;

	@Setter(AccessLevel.NONE)
	private MapTemplate template = new MapTemplate();

	@Override
	protected RedisFuture<?> write(Map<String, Object> record, RediSearchAsyncCommands<String, String> commands) {
		String query = query(record);
		log.debug("Aggregate index={}, query='{}' options={}", index, query, options);
		return commands.aggregate(index, query, options);
	}

	private String query(Map<String, Object> record) {
		return template.expand(query, record);
	}

}
