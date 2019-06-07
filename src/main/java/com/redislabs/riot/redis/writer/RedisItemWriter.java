package com.redislabs.riot.redis.writer;

import java.util.Map;

import com.redislabs.riot.redis.RedisConverter;

public interface RedisItemWriter {

	Object write(Object redis, Map<String, Object> item);

	void setConverter(RedisConverter redisConverter);

	void setCommands(RedisCommands build);

}
