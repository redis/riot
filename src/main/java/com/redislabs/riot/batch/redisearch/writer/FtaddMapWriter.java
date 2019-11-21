package com.redislabs.riot.batch.redisearch.writer;

import java.util.Map;

@SuppressWarnings("unchecked")
public class FtaddMapWriter<R> extends AbstractSearchMapWriter<R> {

	@Override
	protected Object write(R redis, String key, Map<String, Object> item) {
		return commands.ftadd(redis, index, key, score(item), stringMap(item), options);
	}

}
