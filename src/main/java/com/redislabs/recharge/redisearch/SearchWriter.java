package com.redislabs.recharge.redisearch;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.search.SearchOptions;

import io.lettuce.core.RedisFuture;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Setter
public class SearchWriter extends AbstractRediSearchWriter {

	private String query;
	private SearchOptions options;

	@Override
	protected RedisFuture<?> write(Map<String, Object> record, RediSearchAsyncCommands<String, String> commands) {
		String expandedQuery = new MapTemplate().expand(query, record);
		log.debug("Search index={}, query='{}' options={}", index, expandedQuery, options);
		return commands.search(index, expandedQuery, options);
	}

}
