package com.redislabs.riot.redis.writer.search;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.search.AddOptions;

import io.lettuce.core.RedisFuture;

public abstract class AbstractSearchWriter extends AbstractLettuSearchItemWriter {

	private AddOptions options;

	public void setOptions(AddOptions options) {
		this.options = options;
	}

	@Override
	protected RedisFuture<?> write(RediSearchAsyncCommands<String, String> commands, String index,
			Map<String, Object> item) {
		return write(commands, index, item, options);
	}

	protected abstract RedisFuture<?> write(RediSearchAsyncCommands<String, String> commands, String index,
			Map<String, Object> item, AddOptions options);

}
