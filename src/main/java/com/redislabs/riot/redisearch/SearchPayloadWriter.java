package com.redislabs.riot.redisearch;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.search.AddOptions;

import io.lettuce.core.RedisFuture;

public class SearchPayloadWriter extends AbstractSearchWriter {

	private String payloadField;

	public SearchPayloadWriter(String payloadField) {
		this.payloadField = payloadField;
	}

	private String payload(Map<String, Object> item) {
		return convert(item.remove(payloadField), String.class);
	}

	@Override
	protected RedisFuture<?> write(RediSearchAsyncCommands<String, String> commands, String index,
			Map<String, Object> item, AddOptions options) {
		return commands.add(index, key(item), score(item), stringMap(item), options, payload(item));
	}

}
