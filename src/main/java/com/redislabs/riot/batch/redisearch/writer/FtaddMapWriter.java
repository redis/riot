package com.redislabs.riot.batch.redisearch.writer;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.search.AddOptions;

import io.lettuce.core.RedisFuture;

@SuppressWarnings("unchecked")
public class FtaddMapWriter extends AbstractSearchMapWriter {

	@Override
	protected RedisFuture<?> write(RediSearchAsyncCommands<String, String> commands, String index,
			Map<String, Object> item, AddOptions options) {
		return commands.add(index, key(item), score(item), stringMap(item), options);
	}

}
