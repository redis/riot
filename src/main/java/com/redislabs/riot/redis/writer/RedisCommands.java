package com.redislabs.riot.redis.writer;

import java.util.Map;

public interface RedisCommands {

	Object geoadd(Object redis, String key, double longitude, double latitude, String member);

	Object hmset(Object redis, String key, Map<String, Object> item);

	Object rpush(Object redis, String key, String member);

	Object lpush(Object redis, String key, String member);

	Object sadd(Object redis, String key, String member);

	Object set(Object redis, String key, String string);

	Object zadd(Object redis, String key, double score, String member);

	Object xadd(Object redis, String key, String id, Map<String, Object> item, Long maxlen, boolean approximateTrimming);

}
