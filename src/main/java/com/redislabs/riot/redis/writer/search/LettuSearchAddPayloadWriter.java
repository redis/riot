package com.redislabs.riot.redis.writer.search;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.RediSearchReactiveCommands;
import com.redislabs.lettusearch.search.AddOptions;

import io.lettuce.core.RedisFuture;
import lombok.Setter;
import reactor.core.publisher.Mono;

@Setter
public class LettuSearchAddPayloadWriter extends AbstractLettuSearchItemWriter {

	@Setter
	private String payloadField;
	@Setter
	private AddOptions options;

	private String payload(Map<String, Object> item) {
		return convert(item.remove(payloadField), String.class);
	}

	@Override
	protected RedisFuture<?> write(RediSearchAsyncCommands<String, String> commands, String index,
			Map<String, Object> item) {
		return commands.add(index, key(item), score(item), stringMap(item), options, payload(item));
	}

	@Override
	protected Mono<?> write(RediSearchReactiveCommands<String, String> commands, String index,
			Map<String, Object> item) {
		return commands.add(index, key(item), score(item), stringMap(item), options, payload(item));
	}

}
