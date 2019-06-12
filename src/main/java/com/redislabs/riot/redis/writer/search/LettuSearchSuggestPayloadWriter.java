package com.redislabs.riot.redis.writer.search;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.RediSearchReactiveCommands;

import io.lettuce.core.RedisFuture;
import lombok.Setter;
import reactor.core.publisher.Mono;

public class LettuSearchSuggestPayloadWriter extends LettuSearchSuggestWriter {

	@Setter
	private String payloadField;

	private String payload(Map<String, Object> item) {
		if (payloadField == null) {
			return null;
		}
		return convert(item.remove(payloadField), String.class);
	}

	@Override
	protected RedisFuture<?> sugadd(RediSearchAsyncCommands<String, String> commands, String index, String string,
			double score, boolean increment, Map<String, Object> item) {
		return commands.sugadd(index, string, score, increment, payload(item));
	}

	@Override
	protected Mono<?> sugadd(RediSearchReactiveCommands<String, String> commands, String index, String string,
			double score, boolean increment, Map<String, Object> item) {
		return commands.sugadd(index, string, score, increment, payload(item));
	}

}
