package com.redislabs.riot.redis.writer.search;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.search.AddOptions;

import io.lettuce.core.RedisFuture;
import lombok.Setter;

@Setter
public class LettuSearchAddWriter extends AbstractLettuSearchItemWriter {

	@Setter
	private AddOptions options;

	@Override
	protected RedisFuture<?> write(RediSearchAsyncCommands<String, String> commands, String index,
			Map<String, Object> item) {
		return commands.add(index, key(item), score(item), stringMap(item), options);
	}

}
