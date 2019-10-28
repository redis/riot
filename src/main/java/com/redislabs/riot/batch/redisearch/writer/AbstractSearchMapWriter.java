package com.redislabs.riot.batch.redisearch.writer;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.search.AddOptions;

import io.lettuce.core.RedisFuture;

public abstract class AbstractSearchMapWriter extends AbstractLettuSearchMapWriter {

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

	@Override
	public String toString() {
		return String.format("RediSearch index %s", getIndex());
	}

}
