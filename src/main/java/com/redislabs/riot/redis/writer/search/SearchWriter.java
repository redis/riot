package com.redislabs.riot.redis.writer.search;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.search.AddOptions;

import io.lettuce.core.RedisFuture;

public class SearchWriter extends AbstractSearchWriter {

	public SearchWriter(AddOptions options) {
		super(options);
	}

	@Override
	protected RedisFuture<?> write(RediSearchAsyncCommands<String, String> commands, String index,
			Map<String, Object> item, AddOptions options) {
		return commands.add(index, key(item), score(item), stringMap(item), options);
	}

}
