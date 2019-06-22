package com.redislabs.riot.redis.writer.search;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;

import io.lettuce.core.RedisFuture;
import lombok.Setter;

@Setter
public class SearchPayloadWriter extends AbstractSearchWriter {

	@Setter
	private String payloadField;

	private String payload(Map<String, Object> item) {
		return convert(item.remove(payloadField), String.class);
	}

	@Override
	protected RedisFuture<?> write(RediSearchAsyncCommands<String, String> commands, String index,
			Map<String, Object> item) {
		return commands.add(index, key(item), score(item), stringMap(item), options, payload(item));
	}

}
