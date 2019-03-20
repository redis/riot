package com.redislabs.recharge.redisearch.search;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.search.Limit;
import com.redislabs.lettusearch.search.Limit.LimitBuilder;
import com.redislabs.lettusearch.search.SearchOptions;
import com.redislabs.lettusearch.search.SearchOptions.SearchOptionsBuilder;
import com.redislabs.lettusearch.search.SortBy;
import com.redislabs.recharge.MapTemplate;
import com.redislabs.recharge.redisearch.RediSearchCommandWriter;

import io.lettuce.core.RedisFuture;
import lombok.extern.slf4j.Slf4j;

@SuppressWarnings({ "rawtypes", "unchecked" })
@Slf4j
public class SearchWriter extends RediSearchCommandWriter<SearchConfiguration> {

	private MapTemplate template = new MapTemplate();

	public SearchWriter(SearchConfiguration config) {
		super(config);
	}

	@Override
	protected RedisFuture<?> write(Map record, RediSearchAsyncCommands<String, String> commands) {
		String query = template.expand(config.getQuery(), record);
		SearchOptions options = searchOptions();
		log.debug("Search index={}, query='{}' options={}", config.getIndex(), query, options);
		return commands.search(config.getIndex(), query, options);
	}

	private SearchOptions searchOptions() {
		SearchOptionsBuilder builder = SearchOptions.builder();
		if (config.getLimit() != null) {
			LimitBuilder limit = Limit.builder();
			limit.num(config.getLimit().getNum());
			limit.offset(config.getLimit().getOffset());
			builder.limit(limit.build());
		}
		builder.language(config.getLanguage());
		builder.noContent(config.isNoContent());
		builder.noStopWords(config.isNoStopWords());
		if (config.getSortBy() != null) {
			builder.sortBy(SortBy.builder().field(config.getSortBy().getField())
					.direction(config.getSortBy().getDirection()).build());
		}
		builder.verbatim(config.isVerbatim());
		builder.withPayloads(config.isWithPayloads());
		builder.withScores(config.isWithScores());
		return builder.build();
	}

}
