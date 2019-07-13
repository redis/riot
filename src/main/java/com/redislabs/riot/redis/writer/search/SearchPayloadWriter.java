package com.redislabs.riot.redis.writer.search;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.search.AddOptions;

import io.lettuce.core.RedisFuture;
import lombok.Setter;

@Setter
public class SearchPayloadWriter extends AbstractSearchWriter {

	public SearchPayloadWriter(AddOptions options) {
		super(options);
	}

	@Setter
	private String payloadField;

	private String payload(Map<String, Object> item) {
		return convert(item.remove(payloadField), String.class);
	}

	@Override
	protected RedisFuture<?> write(RediSearchAsyncCommands<String, String> commands, String index,
			Map<String, Object> item, AddOptions options) {
		return commands.add(index, key(item), score(item), stringMap(item), options, payload(item));
	}

}
